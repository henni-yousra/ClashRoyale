package crtracker;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;

import scala.Tuple2;

public class CRTools {

    // Définition d'une constante pour le nombre de semaines
    public static final int WEEKS = 9;

    /**
     * Récupère les batailles distinctes brutes à partir des données Clash Royale.
     * Les données sont filtrées en fonction des dates et du nombre de semaines spécifié.
     * Les batailles sont ensuite distinctes et nettoyées en supprimant les doublons
     * et en triant les événements par date.
     *
     * @param sc    Le contexte Spark utilisé pour lire les données.
     * @param weeks Le nombre de semaines pour lesquelles les données doivent être extraites.
     * @return Un JavaRDD contenant des objets Battle nettoyés.
     */
    public static JavaRDD<Battle> getDistinctRawBattles(JavaSparkContext sc, int weeks) {
        // Charger les données JSON contenant les batailles
        JavaRDD<String> rawBattles = sc.textFile("/user/auber/data_ple/clashroyale2024/clash_big.nljson")
                                       .filter(line -> !line.isEmpty()); // Ignorer les lignes vides

        // Transformer les lignes JSON en objets Battle avec des clés uniques
        JavaRDD<Battle> distinctBattles = rawBattles.mapToPair(line -> {
            Gson gson = new Gson();
            Battle battle = gson.fromJson(line, Battle.class);
            String u1 = battle.players.get(0).utag;
            String u2 = battle.players.get(1).utag;

            // Générer une clé unique basée sur la date, le round et les utilisateurs
            String uniqueKey = battle.date + "_" + battle.round + "_"
                    + (u1.compareTo(u2) < 0 ? u1 + u2 : u2 + u1);
            return new Tuple2<>(uniqueKey, battle);
        }).distinct().values(); // Supprimer les doublons et récupérer les batailles

        // Calculer la fenêtre temporelle en fonction des semaines spécifiées
        Instant slidingWindowStart = Instant.now().minusSeconds(3600 * 24 * 7 * weeks);
        Instant collectionStart = Instant.parse("2024-09-26T09:00:00Z");

        // Filtrer les batailles selon la fenêtre temporelle définie
        JavaRDD<Battle> filteredBattles = distinctBattles.filter(battle -> {
            Instant battleTime = Instant.parse(battle.date);
            return battleTime.isAfter(slidingWindowStart) && battleTime.isAfter(collectionStart);
        });

        // Grouper les batailles par clé unique (round, joueurs, élixir)
        JavaPairRDD<String, Iterable<Battle>> groupedBattles = filteredBattles.mapToPair(battle -> {
            String u1 = battle.players.get(0).utag;
            String u2 = battle.players.get(1).utag;
            double e1 = battle.players.get(0).elixir;
            double e2 = battle.players.get(1).elixir;

            // Générer une clé pour regrouper les batailles similaires
            String groupKey = battle.round + "_"
                    + (u1.compareTo(u2) < 0 ? u1 + e1 + u2 + e2 : u2 + e2 + u1 + e1);
            return new Tuple2<>(groupKey, battle);
        }).groupByKey(); // Regrouper par clé

        // Nettoyer les batailles en triant par date et en éliminant les doublons temporels
        JavaRDD<Battle> cleanedBattles = groupedBattles.values().flatMap(battleGroup -> {
            List<Battle> sortedBattles = new ArrayList<>();
            List<Battle> cleanedList = new ArrayList<>();

            // Ajouter toutes les batailles à une liste temporaire
            battleGroup.forEach(sortedBattles::add);

            // Trier les batailles par ordre croissant de date
            sortedBattles.sort((b1, b2) -> Instant.parse(b1.date).compareTo(Instant.parse(b2.date)));

            // Ajouter la première bataille triée à la liste nettoyée
            cleanedList.add(sortedBattles.get(0));

            // Ajouter les batailles suivantes seulement si elles sont suffisamment éloignées dans le temps
            for (int i = 1; i < sortedBattles.size(); i++) {
                long prevTime = Instant.parse(sortedBattles.get(i - 1).date).getEpochSecond();
                long currTime = Instant.parse(sortedBattles.get(i).date).getEpochSecond();

                // Écart minimal de 10 secondes entre deux batailles
                if (Math.abs(currTime - prevTime) > 10) {
                    cleanedList.add(sortedBattles.get(i));
                }
            }

            // Retourner les batailles nettoyées sous forme d'itérateur
            return cleanedList.iterator();
        });

        // Retourner les batailles nettoyées
        return cleanedBattles;
    }
}
