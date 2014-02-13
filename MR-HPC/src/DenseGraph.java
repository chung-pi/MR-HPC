import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class DenseGraph {

	/**
	 * Generate a dense graph
	 * 
	 * @param fileName Filename
	 * @param vertex Number of vertexs
	 * @throws IOException
	 */
	public DenseGraph(String fileName, int vertex, int edge) throws IOException{
		FileWriter fw = new FileWriter(new File(fileName));
		BufferedWriter out = new BufferedWriter(fw);
		
		for (int i=0; i < vertex; i++){
			out.write(i + ": ");
			for (int j=i+1; j <= i + edge; j++){
				if (j != i){
					out.write(j%vertex + " ");
				}
			}
			out.write("-1\n");
		}
		
		out.close();
		fw.close();
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//new DenseGraph("/Users/chung/Desktop/Dropbox/Eclipse/Ubuntu-MPI/MR-HPC/src/dense.txt", 10000, 10000);
		new DenseGraph(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
	}

}
