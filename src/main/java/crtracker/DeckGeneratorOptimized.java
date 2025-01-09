package crtracker;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class DeckGeneratorOptimized {

    /**
     * Factorielle (ici n <= 8, donc OK).
     */
    private static int factorial(int num) {
        if (num < 2) {
            return 1;
        }
        return num * factorial(num - 1);
    }

    /**
     * Calcul de C(n, k).
     */
    public static int combinations(int n, int k) {
        int numerator = factorial(n);
        int denominator = factorial(k) * factorial(n - k);
        return Math.round((float) numerator / denominator);
    }

    /**
     * Génère toutes les combinaisons de k éléments parmi n.
     */
    public static ArrayList<ArrayList<Integer>> generateCombinations(int n, int k) {
        ArrayList<Integer> elements = new ArrayList<>();
        for (int x = 0; x < n; ++x) {
            elements.add(x);
        }

        ArrayList<ArrayList<Integer>> result = new ArrayList<>();
        generate(new ArrayList<Integer>(), 0, elements, k, result);
        return result;
    }

    private static void generate(ArrayList<Integer> current, int start,
                                 ArrayList<Integer> elements, int k,
                                 ArrayList<ArrayList<Integer>> result) {
        if (current.size() == k) {
            result.add(new ArrayList<>(current));
            return;
        }
        for (int i = start; i < elements.size(); i++) {
            current.add(elements.get(i));
            generate(current, i + 1, elements, k, result);
            current.remove(current.size() - 1);
        }
    }

    /**
     * Traitement pour chaque combinaison : on construit deux Decks (un par joueur),
     * puis on émet deux Tuple2<String, Deck>.
     */
    private static void treatCombination(Battle x,
                                         ArrayList<String> tmp1,
                                         ArrayList<String> tmp2,
                                         List<Tuple2<String, Deck>> res,
                                         ArrayList<Integer> cmb) {

        StringBuilder c1 = new StringBuilder();
        StringBuilder c2 = new StringBuilder();
        for (int i : cmb) {
            c1.append(tmp1.get(i));
            c2.append(tmp2.get(i));
        }

        boolean isFullDeck = (cmb.size() == 8);

        Deck d1 = new Deck(
                c1.toString(),
                isFullDeck ? x.players.get(0).evo : "",
                isFullDeck ? x.players.get(0).tower : "",
                1,
                x.winner,
                x.players.get(0).strength - x.players.get(1).strength,
                x.players.get(0).utag,
                x.players.get(0).league,
                x.players.get(0).ctrophies);

        Deck d2 = new Deck(
                c2.toString(),
                isFullDeck ? x.players.get(1).evo : "",
                isFullDeck ? x.players.get(1).tower : "",
                1,
                1 - x.winner,
                x.players.get(1).strength - x.players.get(0).strength,
                x.players.get(1).utag,
                x.players.get(1).league,
                x.players.get(1).ctrophies);

        res.add(new Tuple2<>(d1.id, d1));
        res.add(new Tuple2<>(d2.id, d2));
    }

    /**
     * Filtre pour écarter les batailles dont on ne veut pas (ex. écart de strength).
     */
    private static boolean isBattleValid(Battle x) {
        // Filtrage sur la longueur du deck
        if (x.players.get(0).deck.length() != 16 || x.players.get(1).deck.length() != 16) {
            return false;
        }
        // Différence trop forte de strength
        if (Math.abs(x.players.get(0).strength - x.players.get(1).strength) > 0.75) {
            return false;
        }
        // Doit être 1v1, etc.
        if (x.players.get(0).touch != 1 || x.players.get(1).touch != 1) {
            return false;
        }
        // Meilleures ligues trop faibles
        if (x.players.get(0).bestleague < 6 || x.players.get(1).bestleague < 6) {
            return false;
        }
        return true;
    }

    /**
     * Filtre sur le deck agrégé : exiger un nombre minimal de players et de battles.
     */
    private static boolean isDeckValid(Deck d, int minPlayers, int minBattles) {
        // On peut faire un double-check, par ex. deck 8 cartes => length==16
        // ...
        if (d.players.size() >= minPlayers && d.count >= minBattles) {
            return true;
        }
        return false;
    }

    /**
     * Comparator pour chercher le top par winrate.
     */
    static class WinrateComparator implements Comparator<Deck>, Serializable {
        @Override
        public int compare(Deck x, Deck y) {
            double wx = (x.count == 0) ? 0.0 : (double) x.win / x.count;
            double wy = (y.count == 0) ? 0.0 : (double) y.win / y.count;
            return Double.compare(wx, wy);
        }
    }

    public static void main(String[] args) throws IOException {

        // Les k-values que l'on veut analyser (sous-decks de taille 4,6,7,8).
        final int[] CARDSGRAMS = {4, 6, 7, 8};
        final int NB_DECKS = 100000;    // top N
        final int PLAYERS = 10;         // min nb de joueurs
        final int BATTLES = 80;         // min nb de batailles

        // Prépare toutes les combinaisons utiles => liste de listes
        ArrayList<ArrayList<ArrayList<Integer>>> combs = new ArrayList<>();
        for (int k : CARDSGRAMS) {
            combs.add(generateCombinations(8, k));
        }

        // Configuration Spark
        SparkConf conf = new SparkConf()
                .setAppName("Deck Generator Optimized")
                // .setMaster("local[*]")  // à adapter selon votre cluster
                ;
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Récupération des battles (RDD). On peut cacher si on le réutilise beaucoup.
        JavaRDD<Battle> clean = CRTools.getDistinctRawBattles(sc, CRTools.WEEKS)
                                       .filter(DeckGeneratorOptimized::isBattleValid)
                                       .persist(StorageLevel.MEMORY_AND_DISK());

        // flatMapToPair => on émet (deckID, Deck) * 2 pour chaque combinaison
        JavaPairRDD<String, Deck> rddDecks = clean.flatMapToPair(battle -> {
            List<Tuple2<String, Deck>> res = new ArrayList<>();
            ArrayList<String> tmp1 = new ArrayList<>(8);
            ArrayList<String> tmp2 = new ArrayList<>(8);

            // Extraction des 8 cartes
            for (int i = 0; i < 8; i++) {
                tmp1.add(battle.players.get(0).deck.substring(i * 2, i * 2 + 2));
                tmp2.add(battle.players.get(1).deck.substring(i * 2, i * 2 + 2));
            }
            // Parcours des combinaisons
            for (int cIndex = 0; cIndex < CARDSGRAMS.length; cIndex++) {
                for (ArrayList<Integer> cmb : combs.get(cIndex)) {
                    treatCombination(battle, tmp1, tmp2, res, cmb);
                }
            }
            return res.iterator();
        });

        // On agrège (reduceByKey) les Decks ayant le même identifiant => merge stats
        JavaPairRDD<String, Deck> reducedDecks = rddDecks
                .reduceByKey((d1, d2) -> d1.merge(d2));

        // Cache pour éviter de refaire le shuffle s'il y a plusieurs actions
        reducedDecks.persist(StorageLevel.MEMORY_AND_DISK());

        // On récupère la valeur (Deck) pour filtrer selon un min de players et battles
        JavaRDD<Deck> stats = reducedDecks
                .values()
                .filter(d -> isDeckValid(d, PLAYERS, BATTLES))
                .persist(StorageLevel.MEMORY_AND_DISK());

        //
        // Au lieu de faire des count() répétés ou un top() direct dans la boucle,
        // on va collecter les "top decks" en local dans une boucle unique.
        //
        List<List<Deck>> topDecksByCards = new ArrayList<>();
        WinrateComparator comparator = new WinrateComparator();

        for (int k : CARDSGRAMS) {
            // Filtre RDD sur la taille du deck = k
            // (deck.id.length() == 2*k).
            JavaRDD<Deck> rddK = stats.filter(d -> d.id.length() / 2 == k);

            // top(...) ramène en local, attention si NB_DECKS est très grand !
            List<Deck> topK = rddK.top(NB_DECKS, comparator);
            topDecksByCards.add(topK);
        }

        //
        // Écriture d'un unique fichier JSON
        // Pour de très gros volumes, on conseillerait plutôt
        // un output distribué (par exemple en parquet, etc.).
        //
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream("best_deck.json"), "utf-8"))) {

            writer.write("{\n");

            boolean firstGlobal = true;
            for (int i = 0; i < CARDSGRAMS.length; i++) {
                int k = CARDSGRAMS[i];
                List<Deck> topList = topDecksByCards.get(i);

                if (!firstGlobal) {
                    writer.write(",\n");
                }
                firstGlobal = false;

                // On écrit le bloc : "4": { "cards": [...], "decks": [...] }
                writer.write("\"" + k + "\": {\n");
                // On peut éventuellement afficher combs.get(i) (liste des indices combinatoires)
                writer.write("\"cards\":" + combs.get(i) + ",\n");
                writer.write("\"decks\":[\n");

                boolean firstDeck = true;
                for (Deck d : topList) {
                    if (!firstDeck) {
                        writer.write(",\n");
                    }
                    firstDeck = false;
                    // toString() => adapter si besoin
                    writer.write(d.toString().replace("'", "\""));
                }
                writer.write("]\n}");
            }

            writer.write("\n}\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

        sc.close();
        System.out.println("OK, terminé !");
    }
}
