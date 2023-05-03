-- Databricks notebook source

create or replace temp view dlt_event_log_view as select * from delta.`dbfs:/pipelines/5a3fefa1-1749-4082-8697-ed25983c25b5/system/events`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select details :autoscale from dlt_event_log_view where details :autoscale <> null

-- COMMAND ----------

SELECT
  timestamp,
  Double(
    case
      when details :autoscale.status = 'RESIZING' then details :autoscale.requested_num_executors
      else null
    end
  ) as starting_num_executors,
  Double(
    case
      when details :autoscale.status = 'SUCCEEDED' then details :autoscale.requested_num_executors
      else null
    end
  ) as succeeded_num_executors,
  Double(
    case
      when details :autoscale.status = 'PARTIALLY_SUCCEEDED' then details :autoscale.requested_num_executors
      else null
    end
  ) as partially_succeeded_num_executors,
  Double(
    case
      when details :autoscale.status = 'FAILED' then details :autoscale.requested_num_executors
      else null
    end
  ) as failed_num_executors
FROM
  dlt_event_log_view
WHERE
  event_type = 'autoscale'
  AND origin.update_id = '${latest_update.id}'
