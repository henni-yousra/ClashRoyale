package crtracker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class DeckGeneratorOptimizedV2 {

    // Pré-calcul des factorielles
    private static final int[] FACTORIAL_CACHE = new int[9];

    static {
        FACTORIAL_CACHE[0] = 1;
        for (int i = 1; i < FACTORIAL_CACHE.length; i++) {
            FACTORIAL_CACHE[i] = FACTORIAL_CACHE[i - 1] * i;
        }
    }

    // Méthode combinatoire avec un itérateur paresseux
    public static Iterator<int[]> generateCombinations(int n, int k) {
        return org.apache.commons.math3.util.CombinatoricsUtils.combinationsIterator(n, k);
    }

    public static void main(String[] args) throws Exception {

        // Paramètres
        final int[] CARDSGRAMS = {4, 6, 7, 8};
        final int NB_DECKS = 100000;
        final int PLAYERS = 10;
        final int BATTLES = 80;

        // Configuration Spark avec Kryo Serializer
        SparkConf conf = new SparkConf()
                .setAppName("Deck Generator Optimized V2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Diffusion des combinaisons en Broadcast
        Broadcast<Map<Integer, List<int[]>>> broadcastCombinations = sc.broadcast(preComputeCombinations(CARDSGRAMS));

        // Chargement et filtrage des batailles
        JavaRDD<Battle> clean = CRTools.getDistinctRawBattles(sc, CRTools.WEEKS)
                .filter(DeckGeneratorOptimizedV2::isBattleValid)
                .repartition(128)
                .persist(StorageLevel.MEMORY_AND_DISK());
                

        // Traitement des combinaisons et génération des decks
        JavaPairRDD<String, Deck> rddDecks = clean.flatMapToPair(battle -> {
            List<Tuple2<String, Deck>> results = new ArrayList<>();
            String[] cards1 = splitDeck(battle.players.get(0).deck);
            String[] cards2 = splitDeck(battle.players.get(1).deck);

            // Parcours des combinaisons pré-calculées
            Map<Integer, List<int[]>> combinations = broadcastCombinations.value();
            for (int k : CARDSGRAMS) {
                for (int[] combination : combinations.get(k)) {
                    treatCombination(battle, cards1, cards2, results, combination);
                }
            }
            return results.iterator();
        });

        // Réduction et filtrage des decks
        JavaPairRDD<String, Deck> reducedDecks = rddDecks.reduceByKey(Deck::merge);
        JavaRDD<Deck> validDecks = reducedDecks.values()
                .filter(d -> isDeckValid(d, PLAYERS, BATTLES))
                .persist(StorageLevel.MEMORY_AND_DISK());

       // Récupération des meilleurs decks pour toutes les tailles de combinaison
        List<Deck> allTopDecks = new ArrayList<>();
        for (int k : CARDSGRAMS) {
            JavaRDD<Deck> filtered = validDecks.filter(d -> d.id.length() / 2 == k);
            List<Deck> topDecks = filtered.top(NB_DECKS, new WinrateComparator());
            allTopDecks.addAll(topDecks);
        }

        // Sauvegarde des meilleurs decks dans un seul fichier JSON
        saveDecksToJson(allTopDecks, "best_deck.json");

        sc.close();
        System.out.println("Traitement terminé !");
    }

    // Pré-calcul des combinaisons pour diffusion en Broadcast
    private static Map<Integer, List<int[]>> preComputeCombinations(int[] cardGrams) {
        Map<Integer, List<int[]>> combinations = new HashMap<>();
        for (int k : cardGrams) {
            List<int[]> combs = new ArrayList<>();
            Iterator<int[]> iterator = generateCombinations(8, k);
            while (iterator.hasNext()) {
                combs.add(iterator.next());
            }
            combinations.put(k, combs);
        }
        return combinations;
    }

    // Méthode pour sauvegarder les decks en JSON
    private static void saveDecksToJson(List<Deck> decks, String filePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(new java.io.File(filePath), decks);
    }

    // Méthode pour découper un deck
    private static String[] splitDeck(String deck) {
        String[] cards = new String[8];
        for (int i = 0; i < 8; i++) {
            cards[i] = deck.substring(i * 2, i * 2 + 2);
        }
        return cards;
    }

    // Validation d'une bataille
    private static boolean isBattleValid(Battle x) {
        return x.players.get(0).deck.length() == 16 &&
                x.players.get(1).deck.length() == 16 &&
                Math.abs(x.players.get(0).strength - x.players.get(1).strength) <= 0.75 &&
                x.players.get(0).touch == 1 &&
                x.players.get(1).touch == 1 &&
                x.players.get(0).bestleague >= 6 &&
                x.players.get(1).bestleague >= 6;
    }

    // Validation d'un deck
    private static boolean isDeckValid(Deck d, int minPlayers, int minBattles) {
        return d.players.size() >= minPlayers && d.count >= minBattles;
    }

    // Traitement des combinaisons
    private static void treatCombination(Battle battle, String[] cards1, String[] cards2, List<Tuple2<String, Deck>> res, int[] combination) {
        StringBuilder c1 = new StringBuilder();
        StringBuilder c2 = new StringBuilder();
        for (int i : combination) {
            c1.append(cards1[i]);
            c2.append(cards2[i]);
        }
        boolean isFullDeck = (combination.length == 8);
        Deck d1 = new Deck(c1.toString(), isFullDeck ? battle.players.get(0).evo : "", isFullDeck ? battle.players.get(0).tower : "", 1, battle.winner, battle.players.get(0).strength - battle.players.get(1).strength, battle.players.get(0).utag, battle.players.get(0).league, battle.players.get(0).ctrophies);
        Deck d2 = new Deck(c2.toString(), isFullDeck ? battle.players.get(1).evo : "", isFullDeck ? battle.players.get(1).tower : "", 1, 1 - battle.winner, battle.players.get(1).strength - battle.players.get(0).strength, battle.players.get(1).utag, battle.players.get(1).league, battle.players.get(1).ctrophies);
        res.add(new Tuple2<>(d1.id, d1));
        res.add(new Tuple2<>(d2.id, d2));
    }

    // Comparator pour le tri par Winrate
    static class WinrateComparator implements Comparator<Deck>, Serializable {
        @Override
        public int compare(Deck x, Deck y) {
            double wx = x.count == 0 ? 0.0 : (double) x.win / x.count;
            double wy = y.count == 0 ? 0.0 : (double) y.win / y.count;
            return Double.compare(wy, wx);
        }
    }
}
