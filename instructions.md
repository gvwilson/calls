To wrap up this workshop, we have created a simple simulation of a call center.
The basic simulation is in the 'plain' database. The data in each of the other
four databases show the results of a shock to the system. Your mission is to
write some SQL queries to figure out what one of those shocks was. The tables
in each database are:

| table     | field             | type     | purpose                      |
| :-------- | :---------------- | :------- | :--------------------------  |
| agent     | ident             | text     | primary key                  |
|           | family            | text     | surname                      |
|           | personal          | text     | forename                     |
| client    | ident             | text     | primary key                  |
|           | family            | text     | surname                      |
|           | personal          | text     | forename                     |
| calls     | call_id           | text     | primary key                  |
|           | client_id         | text     | foreign key                  |
|           | agent_id          | text     | foreign key                  |
|           | call_start        | datetime | when call started            |
|           | call_end          | datetime | when call ended              |
|           | call_duration     | bigint   | duration in minutes          |
|           | rating            | bigint   | client rating of call (1-5)  |
| followups | agent_id          | text     | foreign key                  |
|           | call_id           | text     | foreign key                  |
|           | followup_start    | datetime | when agent followup started  |
|           | followup_end      | datetime | when agent followup ended    |
|           | followup_duration | float    | followup duration in minutes |

Note that `call_duration` and `followup_duration` could be calculated from
start and end times, but are provided here to keep your queries simpler.
