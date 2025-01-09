package crtracker;

import java.time.Instant;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;

import scala.Tuple2;

public class CRTools {

    // Définition d'une constante pour le nombre de semaines (utilisée dans la méthode getDistinctRawBattles)
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

        // Chargement du fichier JSON contenant les données des batailles
        JavaRDD<String> rdd = sc.textFile("/user/auber/data_ple/clashroyale2024/clash_huge.nljson")
                               .filter((x) -> {
                                    // Filtrer les lignes vides
                                    return !x.isEmpty();
                                });

        // Transformation des lignes JSON en objets Battle et application d'une clé unique pour chaque bataille
        JavaRDD<Battle> rddpair = rdd.mapToPair((x) -> {
            // Utilisation de Gson pour convertir chaque ligne en un objet Battle
            Gson gson = new Gson();
            Battle d = gson.fromJson(x, Battle.class);
            String u1 = d.players.get(0).utag;
            String u2 = d.players.get(1).utag;
            // Génération d'une clé unique combinant la date, le round, et les utilisateurs pour garantir que les batailles sont distinctes
            return new Tuple2<>(d.date + "_" + d.round + "_"
                    + (u1.compareTo(u2) < 0 ? u1 + u2 : u2 + u1), d);
        }).distinct().values();

        // Définir une fenêtre temporelle qui commence il y a 'weeks' semaines
        Instant sliding_window = Instant.now().minusSeconds(3600 * 24 * 7 * weeks);
        Instant collect_start = Instant.parse("2024-09-26T09:00:00Z");

        // Filtrer les batailles selon la fenêtre temporelle définie et la date de collecte
        rddpair = rddpair.filter((Battle x) -> {
            Instant inst = Instant.parse(x.date);
            return inst.isAfter(sliding_window) && inst.isAfter(collect_start);
        });

        // Grouper les batailles par clé (round + utilisateurs + elixir)
        /*Pour chaque bataille (d), on extrait les identifiants des deux joueurs (u1, u2) et leurs élixirs respectifs (e1, e2).
        Ensuite, une clé est générée en combinant :
        Le round de la bataille.
        Les identifiants des joueurs (u1 et u2), en les triant pour éviter des doublons dans un ordre spécifique (u1.compareTo(u2)), ce qui garantit que l'ordre des joueurs n'affecte pas la clé.
        Les valeurs d'elixir des joueurs (e1 et e2).
        La clé résultante est utilisée pour grouper les batailles similaires ensemble. Cela permet de s'assurer que toutes les batailles entre les mêmes joueurs, dans le même round, et avec les mêmes quantités d'élixir, sont regroupées.*/
        JavaPairRDD<String, Iterable<Battle>> rddbattles = rddpair.mapToPair((d) -> {
            String u1 = d.players.get(0).utag;
            String u2 = d.players.get(1).utag;
            double e1 = d.players.get(0).elixir;
            double e2 = d.players.get(1).elixir;
            // Génération de la clé pour chaque groupe de batailles
            return new Tuple2<>(d.round + "_"
                    + (u1.compareTo(u2) < 0 ? u1 + e1 + u2 + e2 : u2 + e2 + u1 + e1), d);
        }).groupByKey();//Cette fonction permet de regrouper les batailles qui partagent la même clé en un seul groupe.
      


        // Nettoyer les batailles en triant par date et en éliminant les doublons (basés sur la proximité temporelle)
        JavaRDD<Battle> clean = rddbattles.values().flatMap((it) -> {
            ArrayList<Battle> lbattles = new ArrayList<>();
            ArrayList<Battle> rbattles = new ArrayList<>();
            // Ajouter toutes les batailles à une liste temporaire
            for (Battle bi : it)
                lbattles.add(bi);

            // Trier les batailles par date croissante
            lbattles.sort((Battle x, Battle y) -> {
                if (Instant.parse(x.date).isAfter(Instant.parse(y.date)))
                    return 1;
                if (Instant.parse(y.date).isAfter(Instant.parse(x.date)))
                    return -1;
                return 0;
            });

            // Ajouter la première bataille triée à la liste des résultats
            rbattles.add(lbattles.get(0));/*Après avoir trié les batailles dans lbattles, la première bataille (celle avec la date la plus ancienne) est ajoutée à la liste rbattles, qui va contenir les batailles nettoyées. C'est le point de départ pour commencer à comparer les autres batailles. */

            // Ajouter les batailles suivantes uniquement si elles sont suffisamment distantes dans le temps
            for (int i = 1; i < lbattles.size(); ++i) {//On commence par parcourir la liste lbattles à partir de la deuxième bataille (index 1).
                long i1 = Instant.parse(lbattles.get(i - 1).date).getEpochSecond();
                long i2 = Instant.parse(lbattles.get(i).date).getEpochSecond();
                // Ajouter la bataille si l'écart de temps entre deux batailles successives est supérieur à 10 secondes
                if (Math.abs(i1 - i2) > 10)/*L'écart en secondes entre les deux batailles est calculé avec Math.abs(i1 - i2). Si cet écart est supérieur à 10 secondes, la bataille i est considérée comme suffisamment distante pour être ajoutée à rbattles.*/
                    rbattles.add(lbattles.get(i));
            }
            return rbattles.iterator();//La méthode flatMap attend un itérateur, et c'est ce qui est retourné ici. L'itérateur permettra à Spark de traiter les batailles nettoyées plus loin dans le pipeline.
        });
        
        // Retourner les batailles nettoyées
        return clean;
        /*Après avoir appliqué le nettoyage des batailles 
        (par tri et élimination des doublons temporels), 
        la variable clean contient l'ensemble des batailles finales.
         Cette variable est un JavaRDD<Battle>, qui représente un RDD (Resilient Distributed Dataset) contenant toutes les batailles après le nettoyage. */
    }
}
