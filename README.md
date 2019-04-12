# Quick Start

## Building

$ gradle build

## Running the services

### Run from the IDE

1. Find and execute RunMe_01_Monitor_8080
    * Verify by opening http://127.0.0.1:8080
2. Find and execute RunMe_02_Supervisor_8081
3. Find and execute RunMe_03_Supervisor_8082
4. Find and execute RunMe_04_Supervisor_8083


### Run from the terminal
TODO

## Starting a subscription

    curl http://127.0.0.1:8080/day0/acme -X POST
    
A poll thread will start printing 'Polling acme' in the terminal.

**NOTE:** Any running service can be used 8080, 8081, 8082, 8083     
 
## Stopping a subscription

    curl http://127.0.0.1:8080/day0/acme -X DELETE
    
The poll thread will stop printing.

## Rebalancing

First make sure some subscriptions are running

    > curl http://127.0.0.1/day0/amazon -X POST
    > curl http://127.0.0.1/day0/kfc -X POST
    > curl http://127.0.0.1/day0/tacobell -X POST
    
Verify in the output for one of the supervisors which should look something like this.

    Poll #1517 for kfc
    Poll #1518 for kfc
    Poll #1519 for kfc
    Poll #1520 for kfc
    Poll #1521 for kfc
    Poll #1522 for kfc    
    
Now kill the supervisor hosting kfc. Start checking the output of the other supervisors. One of them will start hosting kfc.

Now restart the supervisor. This will trigger a rebalance and move one or more of the subscriptions onto the new supervisor.

