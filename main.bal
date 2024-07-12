import ballerina/graphql;
import ballerina/sql;
import ballerinax/aws.redshift;
import ballerinax/aws.redshift.driver as _;
import ballerina/log;

configurable string & readonly jdbcUrl = ?;
configurable string & readonly user = ?;
configurable string & readonly password = ?;

configurable int & readonly maxOpenConnections = 80;
configurable int & readonly minIdleConnections = 20;

final redshift:Client dbClient = check new (password = password, user = user, url = jdbcUrl);

configurable int port = 9090;

isolated int i = 0;

service /graphql on new graphql:Listener(9090, timeout = 100) {
    isolated resource function get greeting() returns string|error {
        future<string|error>[] dbCalls = [];
        foreach int i in 0...15 {
            dbCalls[i] = start runDBQuery();
        }

        foreach var item in dbCalls {
            string|error result = wait item;
            if result is error {
                log:printError("Error returned", result);
            }
        }
        return "GraphQL done";
    }
}

isolated function runDBQuery() returns string|error {
    stream<record {}, sql:Error?> albumStream = dbClient->query(`SELECT * FROM users limit 1000`);
    check albumStream.close();
    return "Done";
}
