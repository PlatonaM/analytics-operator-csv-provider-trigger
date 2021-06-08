## analytics-operator-csv-provider-trigger

Trigger [analytics-csv-provider](https://github.com/PlatonaM/analytics-csv-provider) when new data is available.

### Configuration

`message_threshold`: Number of messages to consume until a job is triggered.

`csv_provider_url`: URL of analytics-csv-provider.

`service_id`: ID of the data service for which analytics-csv-provider is to be triggered.

`time_field`: Field containing timestamps.

`delimiter`: Delimiter to use for CSV.

`schedule_job_delay`: Time to wait before a job is scheduled.

`poll_delay`: Determines how often to check if a job that is already running is finished.

`logging_level`: Set logging level to `info`, `warning`, `error` or `debug`.

### Inputs

`time`: Timestamp for the data of a message.
