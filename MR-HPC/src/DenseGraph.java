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
	public DenseGraph(String fileName, int vertex, int edge, int lineEachFile) throws IOException{
		int index = 0;
		FileWriter fw = null;
		BufferedWriter out = null;
		
		for (int i=0; i < vertex; i++){
			if (i % lineEachFile == 0){
				if (index > 0){
					out.close();
					fw.close();
				}
				fw = new FileWriter(new File(fileName + index + ".txt"));
				out = new BufferedWriter(fw);
				index++;
			}
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
		new DenseGraph(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
	}

}
