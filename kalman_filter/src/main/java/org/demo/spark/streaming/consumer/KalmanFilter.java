package org.demo.spark.streaming.consumer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.demo.spark.streaming.UtilsAndConstants;
import java.util.Random;

public class KalmanFilter {
    private static String SUBSCRIBE = "subscribe";
    private static Random OBSERVATION_NOISE_x = new Random(17686267572368l);
    private static Random OBSERVATION_NOISE_y = new Random(40639867672371l);
    private static final double SIGMA_px = UtilsAndConstants.SIGMA_px;
    private static final double SIGMA_py = UtilsAndConstants.SIGMA_py;

    private static DenseMatrix G = UtilsAndConstants.G;
    private static DenseMatrix F = UtilsAndConstants.F;
    private static DenseMatrix F_T = F.transpose();

    private static Matrix R;
    static {
        double[] vals = {Math.pow(SIGMA_px,2),0, 0,Math.pow(SIGMA_py,2)};
        R =  new DenseMatrix(2,2,vals);
    }

    private static Matrix Q;

    static {
        double[] vals = {Math.pow(SIGMA_px,2), 0, 0, 0,
            0, Math.pow(SIGMA_px,2), 0, 0,
                0, 0, Math.pow(SIGMA_py,2), 0,
                0, 0, 0, Math.pow(SIGMA_py,2)};
        Q = (new DenseMatrix(4,4,vals)).multiply(G).multiply(G.transpose());
    }

    private static DenseMatrix H;
    static {
        double[] vals = {1,0, 0,0, 0,1, 0,0};
        H = new DenseMatrix(2, 4,vals);
    }
    private static DenseMatrix H_T = H.transpose();

    private static DenseMatrix P_current;
    static {
        P_current = UtilsAndConstants.scale(DenseMatrix.eye(4),0);
    }

    private static Vector X_current; // Estimated
    static {
        double[] vals = {4,0,0,0};
        X_current = new DenseVector(vals);
    };

    private static Vector getNextX(){
        return F.multiply(X_current);
    }

    private static DenseMatrix getNextP(){
        DenseMatrix nextP = UtilsAndConstants.add(F.multiply(P_current).multiply(F_T),Q);
        return nextP;
    }

    private static DenseVector getObservationNoiseVector(){
        double dx = OBSERVATION_NOISE_x.nextGaussian() * SIGMA_px;
        double dy = OBSERVATION_NOISE_y.nextGaussian() * SIGMA_py;
        double[] d = new double[2];
        d[0] = dx;
        d[1] = dy;
        return new DenseVector(d);
    }

    private static DenseVector getZ(DenseVector y){
        return UtilsAndConstants.addOrSubtract(y, H.multiply(getNextX()),false);
    }

    private static DenseMatrix getS(){
        return UtilsAndConstants.add((H.multiply(getNextP())).multiply(H_T), R);
    }

    private static Matrix getK(){
        return getNextP().multiply(H_T).multiply(UtilsAndConstants.inverse(getS()));
    }

    private static void updateXandP(DenseVector y){
        DenseVector z = getZ(y);
        Matrix K = getK();
        P_current = UtilsAndConstants.add(DenseMatrix.eye(4),
                UtilsAndConstants.scale(K.multiply(H),-1)).multiply(getNextP());
        X_current = UtilsAndConstants.addOrSubtract(getNextX(), K.multiply(z), true);

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: KalmanFilter <bootstrap-server> " +
                    "<topic>");
            System.exit(1);
        }
        Logger.getRootLogger().setLevel(Level.FATAL);


        String bootstrapServer = args[0];
        String topic = args[1];

        SparkSession spark = SparkSession
                .builder()
                .appName("KalmanFilter").master("local[*]")
                .getOrCreate();

        // Create DataSet representing the stream of input lines from kafka
        Dataset<String> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServer)
                .option(SUBSCRIBE, topic)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());



        StreamingQuery query = lines.writeStream().foreach(new ForeachWriter<String>() {

            // open connection
            public boolean open(long partitionId, long version) {
                return true;
            }


            public void process(String value) {
                // Actual
                String[] tokens = value.split(UtilsAndConstants.SEPARATOR_RGX);
                String actual_position_x = tokens[0];
                String actual_speed_x = tokens[1];
                String actual_position_y = tokens[2];
                String actual_speed_y = tokens[3];

                double[] vals = {Double.parseDouble(actual_position_x), Double.parseDouble(actual_speed_x),
                        Double.parseDouble(actual_position_y),Double.parseDouble(actual_speed_y)};
                Vector actual = new DenseVector(vals);

                // Estimated
                double estimated_position_x = (X_current.toArray())[0];
                double estimated_speed_x = (X_current.toArray())[1];
                double estimated_position_y = (X_current.toArray())[2];
                double estimated_speed_y = (X_current.toArray())[3];

                // Observed
                DenseVector observationNoise = getObservationNoiseVector();
                DenseVector observation = UtilsAndConstants.addOrSubtract(H.multiply(actual),observationNoise,true);

                double observed_position_x = (observation.toArray())[0];
                double observed_position_y = (observation.toArray())[1];

                System.out.println(estimated_position_x+"|"+observed_position_x+
                        "|"+actual_position_x+"|"+estimated_position_y+"|"+observed_position_y+"|"+actual_position_y
                        +"|"+estimated_speed_x+"|"+actual_speed_x+"|"+estimated_speed_y+"|"+actual_speed_y);

                updateXandP(observation);
            }


            public void close(Throwable errorOrNull) {
                // close the connection
            }
        }).start();

        query.awaitTermination();
    }

}
