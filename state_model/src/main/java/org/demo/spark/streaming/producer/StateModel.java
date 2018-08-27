package org.demo.spark.streaming.producer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.demo.spark.streaming.UtilsAndConstants;
import java.util.Random;

public class StateModel {
    private static Random PROCESS_NOISE_X = new Random(23423874764L);
    private static Random PROCESS_NOISE_Y = new Random(96653878224L);

    private static DenseMatrix F = UtilsAndConstants.F;
    private static DenseMatrix G = UtilsAndConstants.G;

    private static Vector X_a_current;
    static {
        double[] vals = {0,5,3,1}; // position x, speed x, position y, speed y
        X_a_current = new DenseVector(vals);
    }

    private static void updateXa(){
        double p_x = PROCESS_NOISE_X.nextGaussian() * UtilsAndConstants.SIGMA_px;
        double p_y = PROCESS_NOISE_Y.nextGaussian() * UtilsAndConstants.SIGMA_py;
        double[] vals = {p_x,p_y};
        DenseVector processNoise = new DenseVector(vals);
        X_a_current = UtilsAndConstants.addOrSubtract(F.multiply(X_a_current),
                G.multiply(processNoise), true);
    }


    public static void main(String[] args){
        if (args.length != 2) {
            System.err.println("Usage: StateModel <bootstrap-server> <topic>");
            System.exit(1);
        }
        Logger.getRootLogger().setLevel(Level.FATAL);
        TopicWriter.init(args[0],args[1]);
        for(int i = 0; i < 1500; i++){
            TopicWriter.write(UtilsAndConstants.formatElements(X_a_current));
            updateXa();
        }
        TopicWriter.close();
    }
}
