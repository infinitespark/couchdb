% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(fabric2_db_expiration).


-behaviour(gen_server).


-export([
    start_link/0,
    cleanup/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).


-include_lib("couch/include/couch_db.hrl").
-include_lib("fabric/include/fabric2.hrl").


-define(JOB_TYPE, <<"dbexpiration">>).
-define(JOB_ID, <<"dbexpiration_job">>).
-define(DEFAULT_RETENTION_SEC, 172800). % 48 hours
-define(DEFAULT_EXPIRATION_BATCH, 100).
-define(DEFAULT_SCHEDULE_SEC, 7). % 1 hour
-define(ERROR_RESCHEDULE_SEC, 3).
-define(INITIAL_DELAY_MSEC, 900).
-define(CHECK_ENABLED_MSEC, 2000).
-define(JOB_TIMEOUT_SEC, 30).


-record(st, {
    job
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, nil, []).


init(_) ->
    couch_log:info("~p : in init", [?MODULE]),
    {ok, #st{job = undefined}, ?INITIAL_DELAY_MSEC}.


terminate(_M, _St) ->
    ok.


handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info(timeout, #st{job = undefined} = St) ->
    couch_log:info("~p : got timeout", [?JOB_ID]),
    ok = wait_for_couch_jobs_app(),
    ok = couch_jobs:set_type_timeout(?JOB_TYPE, ?JOB_TIMEOUT_SEC),
    % for testing
    Removed = couch_jobs:remove(undefined, ?JOB_TYPE, ?JOB_ID),
    couch_log:info("~p : remove result ~p", [?MODULE, Removed]),
    ok = maybe_add_job(),
    couch_log:info("~p : added job ~p, initialized", [?MODULE, ?JOB_ID]),
    {_, Ref} = spawn_monitor(?MODULE, cleanup, [is_enabled()]),
    {noreply, St#st{job = Ref}};

handle_info({'DOWN', Ref, _, _, Exit}, #st{job = Ref} = St) ->
    couch_log:info("~p : job finished with ~p", [?MODULE, Exit]),
    case Exit of
        normal -> ok;
        Error -> couch_log:error("~p : job error ~p", [?MODULE, Error])
    end,
    {_, Ref} = spawn_monitor(?MODULE, cleanup, [is_enabled()]),
    {noreply, St#st{job = Ref}};

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


wait_for_couch_jobs_app() ->
    % Because of a circular dependency between couch_jobs and fabric apps, wait
    % for couch_jobs to initialize before continuing. If we refactor the
    % commits FDB utilities out we can remove this bit of code.
    Apps = try
        application:which_applications(1000)
    catch
        _:_ -> []
    end,
    case lists:keysearch(couch_jobs, 1, Apps) of
        {value, {couch_jobs, _, _}} ->
            couch_log:info("~p : couch_jobs started! ", [?MODULE]),
            ok;
        false ->
            timer:sleep(1000),
            couch_log:info("~p : waiting for couch jobs", [?MODULE]),
            wait_for_couch_jobs_app()
    end.


maybe_add_job() ->
    case couch_jobs:get_job_data(undefined, ?JOB_TYPE, ?JOB_ID) of
        {error, not_found} ->
            Now = erlang:system_time(second),
            ok = couch_jobs:add(undefined, ?JOB_TYPE, ?JOB_ID, #{}, Now);
        {ok, _JobData} ->
            ok
    end.


cleanup(false = _Enabled) ->
    couch_log:info("~p : Not enabled waiting ...", [?MODULE]),
    timer:sleep(?CHECK_ENABLED_MSEC),
    exit(normal);

cleanup(true = _Enabled) ->
    couch_log:info("~p : enable, accepting ...", [?MODULE]),
    Opts = #{max_sched_time => erlang:system_time(second), timeout => 1},
    case couch_jobs:accept(?JOB_TYPE, Opts) of
        % maybe handle timeout here, need to check the api
        {ok, Job, Data} ->
            try
                couch_log:error("~p : processing expirations ~p ~p", [?MODULE, Job, Data]),
                {ok, Job1, Data1} = process_expirations(Job, Data),
                couch_log:error("~p : resubmitting job ~p ~p", [?MODULE, Job, schedule_sec()]),
                ok = resubmit_job(Job1, Data1, schedule_sec())
            catch
                _Tag:Error ->
                    Stack = erlang:get_stacktrace(),
                    couch_log:error("~p : processing error ~p ~p ~p", [?MODULE, Job, Error, Stack]),
                    ok = resubmit_job(Job, Data, ?ERROR_RESCHEDULE_SEC),
                    exit({job_error, Error, Stack})
            end;
        {error, not_found} ->
            couch_log:error("~p : not found error", [?MODULE]),
            timer:sleep(1000),
            cleanup(is_enabled())
    end.



resubmit_job(Job, Data, After) ->
    Now = erlang:system_time(second),
    SchedTime = Now + After,
    couch_jobs:finish(undefined, Job, Data),
    couch_jobs:add(undefined, ?JOB_TYPE, ?JOB_ID, Data, SchedTime),
    ok.
    %% couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
    %%      {ok, Job1} = couch_jobs:resubmit(JTx, Job, SchedTime),
    %%      ok = couch_jobs:finish(JTx, Job1, Data)
    %% end),
    %% ok.


process_expirations(#{} = Job, #{} = Data) ->
    % Maybe periodically update the job so it doesn't expire
    Callback = fun(Value, Acc) ->
        NewAcc = case Value of
            {meta, _} -> Acc;
            {row, DbInfo} ->
                process_row(Acc, DbInfo);
            complete ->
                TotalLen = length(Acc),
                if TotalLen == 0 -> Acc; true ->
                    [{LastDelete, _, _} | _] = Acc,
                    TotalLen = length(Acc),
                    delete_dbs(lists:sublist(Acc, TotalLen - LastDelete)),
                    Acc
                end
        end,
        {ok, NewAcc}
    end,
    {ok, _Infos} = fabric2_db:list_deleted_dbs_info(Callback, [], []),
    {ok, Job, Data}.


process_row(Acc, DbInfo) ->
    TotalLen = length(Acc),
    case TotalLen of
        0 ->
            DbName = proplists:get_value(db_name, DbInfo),
            TimeStamp = proplists:get_value(timestamp, DbInfo),
            [{0, DbName, TimeStamp}];
        _ ->
            [{LastDelete, _, _} | _] = Acc,
            NumberToDelete = TotalLen - LastDelete,
            DeleteBatch = expiration_batch(),
            LastDelete2 = case NumberToDelete == DeleteBatch of
                true ->
                    delete_dbs(lists:sublist(Acc, DeleteBatch)),
                    TotalLen;
                _ ->
                    LastDelete
            end,
            DbName = proplists:get_value(db_name, DbInfo),
            TimeStamp = proplists:get_value(timestamp, DbInfo),
            [{LastDelete2, DbName, TimeStamp} | Acc]
    end.


delete_dbs(Infos) ->
    lists:foreach(fun({_, DbName, TimeStamp}) ->
        Now = now_sec(),
        Retention = retention_sec(),
        Since = Now - Retention,
        case Since > timestamp_to_sec(TimeStamp)  of
            true ->
                ok = fabric2_db:delete(DbName, [{deleted_at, TimeStamp}]);
            false ->
                ok
        end
    end, Infos).


now_sec() ->
    Now = os:timestamp(),
    Nowish = calendar:now_to_universal_time(Now),
    calendar:datetime_to_gregorian_seconds(Nowish).


timestamp_to_sec(TimeStamp) ->
    <<Year:4/binary, "-", Month:2/binary, "-", Day:2/binary,
        "T",
        Hour:2/binary, ":", Minutes:2/binary, ":", Second:2/binary,
        "Z">> = TimeStamp,

    calendar:datetime_to_gregorian_seconds(
        {{?bin2int(Year), ?bin2int(Month), ?bin2int(Day)},
            {?bin2int(Hour), ?bin2int(Minutes), ?bin2int(Second)}}
    ).


is_enabled() ->
    config:get_boolean("couch", "db_expiration_enabled", true).


retention_sec() ->
    config:get_integer("couch", "db_expiration_retention_sec",
        ?DEFAULT_RETENTION_SEC).


schedule_sec() ->
    config:get_integer("couch", "db_expiration_schedule_sec",
        ?DEFAULT_SCHEDULE_SEC).


expiration_batch() ->
    config:get_integer("couch", "db_expiration_batch",
        ?DEFAULT_EXPIRATION_BATCH).
