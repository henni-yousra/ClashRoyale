//mvn package;~/spark-3.5.0-bin-hadoop3/bin/spark-submit --master local --deploy-mode client target/TPSpark-0.0.1.jar

/*
 * result
 * [Deck 08355b count: 1311.0 winrate: 64% nbplayers : 101 strengthW: 0.233451536643026, 
 *  Deck 082935 count: 1342.0 winrate: 64% nbplayers : 101 strengthW: 0.2571531791907514, 
 *  Deck 0c2137 count: 1441.0 winrate: 64% nbplayers : 101 strengthW: -0.008802816901408451, 
 *  Deck 2f444f count: 1111.0 winrate: 64% nbplayers : 101 strengthW: -0.008438818565400843, 
 *  Deck 2f4448 count: 1340.0 winrate: 63% nbplayers : 101 strengthW: 0.026848591549295774, 
 *  Deck 292f4f count: 1297.0 winrate: 63% nbplayers : 101 strengthW: 0.009784587378640778, Deck 2f484f count: 1105.0 winrate: 63% nbplayers : 101 strengthW: 0.00641025641025641, Deck 3d5a6d count: 1449.0 winrate: 63% nbplayers : 101 strengthW: 0.1800789760348584, Deck 08646b count: 1394.0 winrate: 63% nbplayers : 101 strengthW: 0.25736126840317103, Deck 0c2940 count: 1633.0 winrate: 63% nbplayers : 101 strengthW: 0.30680164888457806, Deck 022f6d count: 1147.0 winrate: 63% nbplayers : 101 strengthW: 0.271667817679558, Deck 08355e count: 1865.0 winrate: 63% nbplayers : 101 strengthW: 0.21659940526762958, Deck 122f6d count: 1075.0 winrate: 62% nbplayers : 101 strengthW: 0.19534711964549484, Deck 071637 count: 1589.0 winrate: 62% nbplayers : 101 strengthW: 0.2124248496993988, Deck 406265 count: 2575.0 winrate: 62% nbplayers : 101 strengthW: 0.3887600494743352, Deck 0b0c37 count: 7277.0 winrate: 62% nbplayers : 101 strengthW: 0.3228983998246383, Deck 0c3750 count: 2637.0 winrate: 62% nbplayers : 101 strengthW: 0.2405852994555354, Deck 0c2a2b count: 2301.0 winrate: 62% nbplayers : 101 strengthW: 0.17747395833333332, Deck 2a2b37 count: 1948.0 winrate: 62% nbplayers : 101 strengthW: 0.09336483155299918, Deck 071137 count: 4246.0 winrate: 62% nbplayers : 101 strengthW: 0.2523331447963801]
 */

 package crtracker;

 import java.io.BufferedWriter;
 import java.io.FileOutputStream;
 import java.io.IOException;
 import java.io.OutputStreamWriter;
 import java.io.Serializable;
 import java.util.ArrayList;
 import java.util.Comparator;
 import java.util.HashMap;
 
 import org.apache.spark.SparkConf;
 import org.apache.spark.api.java.JavaPairRDD;
 import org.apache.spark.api.java.JavaRDD;
 import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
 
 public class DeckGen {

    private static final int[] FACTORIAL_CACHE = new int[9];

    static {
        FACTORIAL_CACHE[0] = 1;
        for (int i = 1; i < FACTORIAL_CACHE.length; i++) {
            FACTORIAL_CACHE[i] = FACTORIAL_CACHE[i - 1] * i;
        }
    }

    //  private static int factorial(int num) {
    //      if (num < 2) {
    //          return 1;
    //      }
    //      return num * factorial(num - 1);  //possiblité de cache?
    //  }
 
     // Calcul de C(n, k)= n! / (k! * (n - k)!).
     public static int combinations(int n, int k) {
         int numerator = FACTORIAL_CACHE[n];
         int denominator = FACTORIAL_CACHE[k] * FACTORIAL_CACHE[n - k];
         // Le résultat final
         int result = Math.round((float) numerator / denominator);
         return result;
     }
 
     /*Cette méthode génère toutes les combinaisons possibles de k éléments parmi un ensemble de n éléments (ici, les cartes dans un deck). Elle appelle la méthode generate, qui fait le travail récursivement. */
     public static ArrayList<ArrayList<Integer>> generateCombinations(int n, int k) {
         ArrayList<Integer> elements = new ArrayList<Integer>();
         for (int x = 0; x < n; ++x)
             elements.add(x);
 
         ArrayList<ArrayList<Integer>> result = new ArrayList<>();
         generate(new ArrayList<Integer>(), 0, elements, k, result);
         return result;
     }
 
     private static void generate(ArrayList<Integer> current, int start, ArrayList<Integer> elements, int k,
             ArrayList<ArrayList<Integer>> result) {
        /*générer toutes les combinaisons possibles 
        en ajoutant des éléments un à un à la combinaison actuelle, 
        puis en procédant récursivement. */
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
    // private static boolean isDeckValid(Deck d, int minPlayers, int minBattles) {
    //     return d.players.size() >= minPlayers && d.count >= minBattles;
    // }
 /*
 {"date":"2024-09-26T11:29:09Z","game":"gdc","mode":"CW_Battle_1v1","round":0,"type":"riverRacePvP","winner":1,"players":[{"utag":"#YLUGLG29L","ctag":"#YCCQULJ0","trophies":9000,"ctrophies":5066,"exp":57,"league":8,"bestleague":9,"deck":"000b2f3138565a5e","evo":"0b","tower":"","strength":14.4375,"crown":1,"elixir":1.01,"touch":1,"score":100},{"utag":"#JU0VQRQU","ctag":"#YC8R0RJ0","trophies":9000,"ctrophies":5442,"exp":63,"league":9,"bestleague":9,"deck":"0d25374045596062","evo":"3740","tower":"","strength":15.25,"crown":2,"elixir":1.48,"touch":1,"score":200}],"warclan":{"day":3,"hour_seg":0,"period":"112-3","training":[false,false]}}
 {"date":"2024-09-26T11:29:08Z","game":"gdc","mode":"CW_Battle_1v1","round":0,"type":"riverRacePvP","winner":0,"players":[{"utag":"#JU0VQRQU","ctag":"#YC8R0RJ0","trophies":9000,"ctrophies":5442,"exp":63,"league":9,"bestleague":9,"deck":"0d25374045596062","evo":"3740","tower":"","strength":15.25,"crown":2,"elixir":1.48,"touch":1,"score":200},{"utag":"#YLUGLG29L","ctag":"#YCCQULJ0","trophies":9000,"ctrophies":5066,"exp":57,"league":8,"bestleague":9,"deck":"000b2f3138565a5e","evo":"0b","tower":"","strength":14.4375,"crown":1,"elixir":1.01,"touch":1,"score":100}],"warclan":{"day":3,"hour_seg":0,"period":"112-3","training":[false,false]}}
 */
     /*
      * Read battles from the master data sets then compute statistics for each decks
      * subdecks can be also computes (1 2 3 4 ... ) cards by passing the argument.
      * each time statistics about the best evolution and best towers card are
      * provided
      */
 
     public static void main(String[] args) {
         //final int[] CARDSGRAMS = { 4,  6, 7, 8 };
         final int[] CARDSGRAMS = { 7, 8 };
         //final int[] CARDSGRAMS = { 1,  2, 3,  4, 3, 6, 7, 8 };
         final int[] CARDSCOMBI = { 8, 28, 56, 70, 56, 28, 8, 1 };
 
         ArrayList<ArrayList<ArrayList<Integer>>> combs = new ArrayList<ArrayList<ArrayList<Integer>>>();
 
         for (int k : CARDSGRAMS) {
            //génère les combinaisons de cartes pour chaque taille spécifiée dans CARDSGRAMS. 
            // appelle generateCombinations pour générer des sous-ensembles de cartes.
             combs.add(generateCombinations(8, k));
         }
 
         //Configuration Spark
         SparkConf conf = new SparkConf().setAppName("Deck Generator");
         JavaSparkContext sc = new JavaSparkContext(conf);

 
         //récupère les batailles distinctes à partir des données de jeux:
         //JavaRDD<Battle> clean = CRTools.getDistinctRawBattles(sc, CRTools.WEEKS);
         //DeckGenerator utilise CRTools pour obtenir les données de batailles nettoyées avant de procéder à l'analyse des decks. 

         JavaRDD<Battle> clean = CRTools.getDistinctRawBattles(sc, CRTools.WEEKS)
                .filter(DeckGen::isBattleValid)
                .repartition(240);
                System.out.println("Partitions: " + clean.getNumPartitions());

         
         JavaPairRDD<String, Deck> rdddecks = clean.flatMapToPair((x) -> {
            /*Filtrage Supplémentaire :
            Vérifie que les decks des deux joueurs ont une longueur de 16 caractères.
            Assure que la différence de force entre les joueurs n'excède pas 0,75.
            Vérifie que les valeurs touch des deux joueurs sont égales à 1.
            Vérifie que le meilleur rang de ligue des deux joueurs est au moins 6.
            Si l'une de ces conditions n'est pas remplie, la bataille est ignorée (aucune paire clé-valeur n'est générée). */
            //  if (x.players.get(0).deck.length() != 16 || x.players.get(1).deck.length() != 16
            //          || (Math.abs(x.players.get(0).strength - x.players.get(1).strength)) > 0.75
            //          || x.players.get(0).touch != 1 || x.players.get(1).touch != 1
            //          )
            //      return new ArrayList<Tuple2<String, Deck>>().iterator();
 
            //  if ((x.players.get(0).bestleague < 6 || x.players.get(1).bestleague < 6))
            //      return new ArrayList<Tuple2<String, Deck>>().iterator();
 
            /* Découpage des Decks :Chaque deck est divisé en 8 sous-chaînes de 2 caractères chacune, représentant probablement des cartes individuelles.*/
             ArrayList<String> tmp1 = new ArrayList<>();
             ArrayList<String> tmp2 = new ArrayList<>();
             for (int i = 0; i < 8; ++i){
                 tmp1.add(x.players.get(0).deck.substring(i * 2, i * 2 + 2));
                 tmp2.add(x.players.get(1).deck.substring(i * 2, i * 2 + 2));
             }
                 
             ArrayList<Tuple2<String, Deck>> res = new ArrayList<>();
             for (ArrayList<ArrayList<Integer>> aa : combs)
             /*Génération des Combinaisons de Cartes :Pour chaque combinaison de cartes générée précédemment (combs), 
             la méthode treatCombination est appelée pour créer des 
             objets Deck correspondant aux sous-ensembles de cartes sélectionnées.*/
                 for (ArrayList<Integer> cmb : aa)
                     treatCombination(x, tmp1, tmp2, res, cmb);
             return res.iterator();
         });//cache() // persist(StorageLevel.DISK_ONLY);//Le RDD résultant (rdddecks) est mis en cache pour optimiser les performances lors des opérations suivantes.
 
         //System.out.println("rdd decks generated : " + rdddecks.count());
 
         //procède à l'agrégation et au filtrage des statistiques :
         final int PLAYERS = 10;
         final int BATTLES = 80;

         JavaRDD<Deck> stats = rdddecks.reduceByKey((x, y) -> x.merge(y)).values()
            .filter(x -> x.players.size() >= PLAYERS && x.count >= BATTLES);

 
         //reduceByKey agrège les objets Deck ayant la même clé (identifiant du deck) en les fusionnant via la méthode merge.
        //  JavaRDD<Deck> stats = rdddecks.reduceByKey((x, y) -> x.merge(y)).values()
        //          .filter((Deck x) -> {
        //              if (x.id.length() == 16)
        //                  return x.players.size() >= PLAYERS && x.count >= BATTLES;
        //              else
        //                  return x.players.size() >= PLAYERS && x.count >= BATTLES;
        //          });//Seuls les decks ayant été utilisés par au moins PLAYERS (10) joueurs et ayant participé à au moins BATTLES (80) batailles sont conservés.
 

         //System.out.println("rdd decks reduced : " + stats.count());
 
         /*Cette partie regroupe les decks en fonction du nombre de cartes qu'ils contiennent (déterminé par CARDSGRAMS).
            CARDSGRAMS est un tableau contenant les tailles des combinaisons de cartes (par exemple, 4, 6, 7, 8).
            Chaque élément de stats est filtré pour ne garder que les decks ayant cn cartes, où la taille des cartes est déterminée par x.id.length() / 2.
            Ces statistiques sont stockées dans la liste statistics. */
         ArrayList<JavaRDD<Deck>> statistics = new ArrayList<JavaRDD<Deck>>();
         for (int cn : CARDSGRAMS) {
             statistics.add(stats.filter((Deck x) -> x.id.length() / 2 == cn));
             /*Chaque deck possède un ID représentant les cartes qu'il contient.
                La taille du deck est mesurée par la moitié de la longueur de son ID (x.id.length() / 2), car chaque carte est codée sur deux caractères.
                Les decks sont filtrés par taille (4, 6, 7, ou 8 cartes selon CARDSGRAMS), et chaque groupe est ajouté à une liste statistics. */
            }

         /*Comparer deux decks en fonction de leur taux de victoire (winrate).
            Si un deck n'a aucun match enregistré (count == 0), il est considéré comme ayant un score inférieur.
            Les decks ayant un winrate plus élevé sont classés avant ceux ayant un score plus bas. */
        class WinrateComparator implements Comparator<Deck>, Serializable {
             @Override
             public int compare(Deck x, Deck y) {
                 if (y.count == 0 && x.count != 0)
                     return 1;
                 if (x.count == 0 && y.count != 0)
                     return -1;
                 if (x.count == 0 && y.count == 0)
                     return 0;
                /*Si les deux decks ont des données valides, leur winrate est calculé comme le ratio des victoires sur les parties totales (x.win / x.count).
                Les decks sont triés du meilleur au moins bon taux de victoire. */
                 if ((double) x.win / x.count > (double) y.win / y.count)
                     return 1;
                 else if ((double) x.win / x.count < (double) y.win / y.count)
                     return -1;
                 return 0;
             }
          }

        
        
 
         final int NB_DECKS = 100000;
 
         BufferedWriter writer = null;


         //TODO
            // Gson gson = new Gson();
            // try (Writer writer = new BufferedWriter(new OutputStreamWriter(
            //         new FileOutputStream("best_deck.json"), "utf-8"))) {
            //     writer.write(gson.toJson(yourDataStructure));
            // }

 
         try {
             writer = new BufferedWriter(new OutputStreamWriter(
                     new FileOutputStream("best_deck.json"), "utf-8"));
             writer.write("{\n");//Sauvegarder les meilleurs decks pour chaque taille de combinaison (CARDSGRAMS) dans le fichier JSON best_deck.json.
 
             boolean firsta = true;
 
             for (int i = 0; i < CARDSGRAMS.length; ++i) {
                 //System.out.println("kgram " + i + " > " + statistics.get(i).count());
             }
             
             /*Générer un fichier JSON contenant les meilleurs decks pour chaque taille spécifiée dans CARDSGRAMS.
                Les decks sont triés par taux de victoire à l’aide du comparateur WinrateComparator.
                Seuls les NB_DECKS (100 000) meilleurs decks sont inclus pour chaque taille.
                Les informations des decks sont formatées en JSON pour être lisibles et réutilisables*/
             for (int i = 0; i < CARDSGRAMS.length; ++i) {
                 if (!firsta)
                     writer.write(",\n");
                 firsta = false;
                 writer.write("\"" + CARDSGRAMS[i] + "\": {\n");
                 writer.write("\"cards\":" + combs.get(i) + ",\n");
                 writer.write("\"decks\":[\n");
                 boolean first = true;
                 for (Deck d : statistics.get(i).top(NB_DECKS, new WinrateComparator())) {
                     if (!first)
                         writer.write(",\n");
                     first = false;
                     writer.write((d.toString()).replace("'", "\""));//Si ce n'est pas le premier élément (firsta), une virgule et un saut de ligne sont ajoutés.
                 }
                 writer.write("]\n}");
             }
             writer.write("}\n");
         } catch (IOException ex) {
             // Report
         } finally {
             try {
                 writer.close();
             } catch (Exception ex) {
                 /* ignore */}
         }
         System.out.println("OK !!!!!!!!!!!!");
         sc.close();
     }
 
     private static void treatCombination(Battle x, ArrayList<String> tmp1, ArrayList<String> tmp2,
             ArrayList<Tuple2<String, Deck>> res, ArrayList<Integer> cmb) {
         StringBuffer c1 = new StringBuffer();
         StringBuffer c2 = new StringBuffer();
         for (int i : cmb) {
             c1.append(tmp1.get(i));
             c2.append(tmp2.get(i));
         }
         //Les cartes des combinaisons ont concaténées en deux chaînes c1 et c2.
         Deck d1;
         Deck d2;
     if (cmb.size() == 8) {		
         d1 = new Deck(c1.toString(), x.players.get(0).evo, x.players.get(0).tower, 1, x.winner,
                 x.players.get(0).strength - x.players.get(1).strength, x.players.get(0).utag,
                 x.players.get(0).league, x.players.get(0).ctrophies);
         d2 = new Deck(c2.toString(), x.players.get(1).evo, x.players.get(1).tower, 1, 1 - x.winner,
                 x.players.get(1).strength - x.players.get(0).strength, x.players.get(1).utag,
                 x.players.get(1).league, x.players.get(1).ctrophies);
     }
     else {
         d1 = new Deck(c1.toString(), "", "", 1, x.winner,
                 x.players.get(0).strength - x.players.get(1).strength, x.players.get(0).utag,
                 x.players.get(0).league, x.players.get(0).ctrophies);
         d2 = new Deck(c2.toString(), "", "", 1, 1 - x.winner,
                 x.players.get(1).strength - x.players.get(0).strength, x.players.get(1).utag,
                 x.players.get(1).league, x.players.get(1).ctrophies);
     }
     //les deux objets Deck sont créés en fonction de la taille de cmb (8 cartes ou moins).
         res.add(new Tuple2<>(d1.id, d1));
         res.add(new Tuple2<>(d2.id, d2)); 
         //Les objets Deck sont ajoutés à la liste res sous forme de paires clé-valeur.
     }
 
 }
 