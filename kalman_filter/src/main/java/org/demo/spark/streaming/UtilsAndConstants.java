package org.demo.spark.streaming;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.linalg.Vector;

public class UtilsAndConstants {
    public static final double DT = 0.01;
    public static final String SEPARATOR = "^";
    public static final String SEPARATOR_RGX = "\\^";
    public static final double SIGMA_px = 5;
    public static final double SIGMA_py = 4;

    public static DenseMatrix G;
    static {
        double vals[] = {Math.pow(UtilsAndConstants.DT,2)/2,UtilsAndConstants.DT,0,0,
                0,0,Math.pow(UtilsAndConstants.DT,2)/2,UtilsAndConstants.DT};
        G = new DenseMatrix(4,2, vals);
    }

    public static DenseMatrix F;
    static {
        double[] vals = {1,0,0,0,UtilsAndConstants.DT,1,0,0,0,0,1,0,0,0,UtilsAndConstants.DT,1};
        F = new DenseMatrix(4,4,vals);
    }

    public static void printMatrix(Matrix test){
        System.out.println("++++++++++++++++");
        double[] tmp = test.toArray();
        for(int i = 0; i < tmp.length; i++){
            System.out.println(tmp[i]);
        }
    }


    public static DenseMatrix inverse(DenseMatrix input){
        int size = input.numCols();
        return new DenseMatrix(size,size,
                ((new Jama.Matrix(input.values(),size)).inverse())
                        .getColumnPackedCopy());
    }

    public static DenseVector addOrSubtract(Vector a, Vector b, boolean isAdd){
        double[] tmp1 = a.toArray();
        double[] tmp2 = b.toArray();
        double[] tmp3 = new double[tmp1.length];
        int factor = 1;
        if(!isAdd){
            factor = -1;
        }
        for(int i = 0; i < tmp1.length; i++){
            tmp3[i] = tmp1[i] + factor*tmp2[i];
        }
        return new DenseVector(tmp3);
    }

    public static DenseMatrix scale(DenseMatrix input, double factor){
        double[] vals = input.toArray();
        double[] newVals = new double[vals.length];
        for(int i = 0; i < vals.length; i++){
            newVals[i] = factor * vals[i];
        }
        return new DenseMatrix(input.numRows(), input.numCols(), newVals);
    }

    public static String formatElements(Vector vector){

        double[] tmp = vector.toArray();
        StringBuilder bldr = new StringBuilder();
        for(int i = 0; i < tmp.length; i++){
            bldr.append(tmp[i]).append(SEPARATOR);
        }
        String tmpStr = bldr.toString();
        return (tmpStr).substring(0,tmpStr.length() - 1);
    }

    public static DenseMatrix add(Matrix a, Matrix b) throws RuntimeException{
        if(a.numRows() != b.numRows() || a.numCols() != b.numCols()){
            throw new RuntimeException("Matrices with incompatible dimensions cannot be added!");
        }
        double[] tmp1 = a.toArray();
        double[] tmp2 = b.toArray();
        double[] tmp3 = new double[tmp1.length];
        for(int i = 0; i < tmp1.length; i++){
            tmp3[i] = tmp1[i] + tmp2[i];
        }
        return new DenseMatrix(a.numRows(),a.numCols(),tmp3);
    }
}
