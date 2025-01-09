package crtracker;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.collection.JavaConverters;
import scala.collection.Map;
import scala.Tuple2;

import java.util.Map.Entry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ClusterInfo {
    public static void main(String[] args) {
        // Configuration Spark
        SparkConf conf = new SparkConf().setAppName("ClusterInfo").setMaster("yarn"); // Exemple en mode YARN
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Récupérer les informations sur la mémoire des exécuteurs
        Map<String, scala.Tuple2<Object, Object>> executorMemoryStatus =
            sc.sc().getExecutorMemoryStatus();

        // Convertir Scala Map en Java Map
        java.util.Map<String, scala.Tuple2<Object, Object>> javaExecutorMemoryStatus =
            JavaConverters.mapAsJavaMap(executorMemoryStatus);

        // Afficher les informations
        for (Map.Entry<String, scala.Tuple2<Object, Object>> entry : javaExecutorMemoryStatus.entrySet()) {
            String executorId = entry.getKey();
            scala.Tuple2<Object, Object> memoryInfo = entry.getValue();

            long totalMemory = (long) memoryInfo._1(); // Mémoire totale
            long usedMemory = (long) memoryInfo._2();  // Mémoire utilisée

            System.out.println("Exécuteur : " + executorId);
            System.out.println("Mémoire totale : " + totalMemory + " octets");
            System.out.println("Mémoire utilisée : " + usedMemory + " octets");
        }

        // Fermer le contexte Spark
        sc.close();
    }
}
