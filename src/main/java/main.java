import Utility.DynamoDBHelper;
import discord4j.common.util.Snowflake;
import discord4j.core.DiscordClient;
import discord4j.discordjson.json.ChannelData;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Duration;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class main {

    public static void main(final String[] args) {

        String token = System.getenv("DiscordToken");
        DiscordClient client = DiscordClient.create(token);
        DynamoDBHelper dynamoDBHelper = new DynamoDBHelper(DynamoDbClient.create());
        DiscordEventListener discordEventListener = new DiscordEventListener(client, dynamoDBHelper);
        discordEventListener.beginListen();

        /*TODO:
        - Add more options
        - Add an option to lock on a timer
        - Add an option for creators to set odds
        - Add premium sub
         */
    }
}
