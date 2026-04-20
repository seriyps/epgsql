-record(repl,
        {
          last_received_lsn :: integer() | undefined,
          last_flushed_lsn :: integer() | undefined,
          last_applied_lsn :: integer() | undefined,
          feedback_required :: boolean() | undefined,
          cbmodule :: module() | undefined,
          cbstate :: any() | undefined,
          receiver :: pid() | undefined,
          align_lsn :: boolean() | undefined,
          standby_status_update_interval = 0 :: integer(),
          last_standby_status_update_time = 0 :: integer()
        }).
