package crtracker;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;

import scala.Tuple2;

public class CRTools {
    public static final int WEEKS = 9;

    public static JavaRDD<Battle> getDistinctRawBattles(JavaSparkContext sc, int weeks) {
        JavaRDD<String> rawData = sc.textFile("./data_ple/clashroyale2024/clash_big.nljson")
                                    .filter(line -> !line.isEmpty());

        JavaPairRDD<String, Battle> distinctBattles = rawData.mapToPair(line -> {
            Gson gson = new Gson();
            Battle battle = gson.fromJson(line, Battle.class);
            String player1 = battle.players.get(0).utag;
            String player2 = battle.players.get(1).utag;
            String uniqueKey = battle.date + "_" + battle.round + "_" +
                    (player1.compareTo(player2) < 0 ? player1 + player2 : player2 + player1);
            return new Tuple2<>(uniqueKey, battle);
        }).reduceByKey((battle1, battle2) -> battle1);

        Instant slidingWindowThreshold = Instant.now().minusSeconds(3600 * 24 * 7 * weeks);
        Instant collectionStartDate = Instant.parse("2024-09-26T09:00:00Z");

        JavaRDD<Battle> filteredBattles = distinctBattles.values().filter(battle -> {
            Instant battleDate = Instant.parse(battle.date);
            return battleDate.isAfter(slidingWindowThreshold) && battleDate.isAfter(collectionStartDate);
        });

        JavaPairRDD<String, Iterable<Battle>> groupedBattles = filteredBattles.mapToPair(battle -> {
            String player1 = battle.players.get(0).utag;
            String player2 = battle.players.get(1).utag;
            double elixir1 = battle.players.get(0).elixir;
            double elixir2 = battle.players.get(1).elixir;
            String groupKey = battle.round + "_" +
                    (player1.compareTo(player2) < 0 ? player1 + elixir1 + player2 + elixir2 : player2 + elixir2 + player1 + elixir1);
            return new Tuple2<>(groupKey, battle);
        }).groupByKey();

        JavaRDD<Battle> cleanedBattles = groupedBattles.values().flatMap(battles -> {
            List<Battle> battleList = new ArrayList<>();
            battles.forEach(battleList::add);

            battleList.sort(Comparator.comparing(battle -> Instant.parse(battle.date)));

            List<Battle> result = new ArrayList<>();
            result.add(battleList.get(0));
            for (int i = 1; i < battleList.size(); i++) {
                long previousTime = Instant.parse(battleList.get(i - 1).date).getEpochSecond();
                long currentTime = Instant.parse(battleList.get(i).date).getEpochSecond();
                if (Math.abs(currentTime - previousTime) > 10) {
                    result.add(battleList.get(i));
                }
            }
            return result.iterator();
        });

        return cleanedBattles;
    }
}
