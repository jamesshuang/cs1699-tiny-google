/**
 * TinyGoogle.java
 *
 * Reads in the output of WordCount.java's MapReduce,
 * and allows users to query the output data
 * 
 * To compile:
 * hadoop com.sun.tools.javac.Main TinyGoogle.java
 * jar cf tg.jar TinyGoogle*.class
 * 
 * To run:
 * hadoop jar tg.jar TinyGoogle
 *
 */
 
import java.util.*;
import java.lang.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class TinyGoogle {

  /**
   * Inverted Index object to support our queries
   */
  public static class InvertedIndex {
    HashMap index;
    
    public InvertedIndex() {
      index = new HashMap();
    }
    
    /**
     * Places the indexes of a word into the Inverted Index
     *
     * @param word the word we are placing in the inverted index
     * @param list ArrayList of IIElements containing the filename and frequency of the word
     */
    public void put(String word, ArrayList<IIElement> list) {
      index.put(word, list);
    }
    
    /**
     * Method that returns ArrayList contain IIElements of a specified word
     *
     * @param key the word we are searching for
     * @return ArrayList<IIElement> ArrayList containing all the IIElements associated with word
     */
    public ArrayList<IIElement> get(String key) {
      return (ArrayList<IIElement>)index.get(key);
    }
    
  }
  
  /*
   * Custome object to store each element in our inverted Index
   */
  public static class IIElement {
    private String filename;
    private int freq;
    
    public void setFilename(String filename) {
      this.filename = filename;
    }
    
    public void setFreq(int freq) {
      this.freq = freq;
    }
    
    public int getFreq() {
      return freq;
    }
    
    public String getFilename() {
      return filename;
    }
        
    /**
     * Compares two elements in the Inverted Index
     *
     * @param o the element we want to compare this element to
     * @return postive int if this object is greater than object being passed, negative int if
     * less than, and zero is two objects are equals
     */
    public int compareTo(IIElement o) {
      int thisValue = this.freq;
      int thatValue = o.getFreq();
      return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
    
    public String toString() {
      return filename + " " + freq + " ";
    }
    
  }
  
  public static void main(String[] args) throws Exception {
    Scanner in = new Scanner(System.in);
    System.out.println("Welcome to Tiny Google!");
    System.out.print("Please enter your hadoop username (same as Pitt email): ");
    String user = in.next();
    System.out.print("Please enter the directory of your MapReduce output in HDFS: ");
    String dir = in.next();
    
    //create path of our output directory in hdfs
    Path p = new Path("hdfs://had6110.cs.pitt.edu:8020/user/" + user + "/" + dir + "/part-r-00000");
    System.out.println("Loading input from: " + p.toString());
    
    FileSystem fs = FileSystem.get(new Configuration());
    
    BufferedReader br = null;
    try {
      //read file from specified path into buffer
      br = new BufferedReader(new InputStreamReader(fs.open(p)));
    } catch(Exception e) {
      System.out.println("Error reading from output directory");
      System.out.println(e);
      System.exit(1);
    }
    
    //initalize our InvertedIndex object that will be used to store and query our words
    InvertedIndex invertedIndex = new InvertedIndex();
    
    //go through our buffer and load all words into our inverted index
    while (br.ready()) {
      String line = br.readLine();
      line = line.replace("\t", " ");
      String[] parse = line.split(" ");
      
      String key = parse[0];
      ArrayList<IIElement> indexes = new ArrayList<>();
      
      for (int i = 1; i < parse.length; i = i + 2) {
        IIElement newElement = new IIElement();
        newElement.setFilename(parse[i]);
        newElement.setFreq(Integer.parseInt(parse[i+1]));
        indexes.add(newElement);
      }
      
      invertedIndex.put(key, indexes);
    }
    
    //At this point, all our words are loaded into our invertedIndex.
    //Our words are ready to be queried
    while(true) {
      System.out.println("\nTinyGoogle:");
      System.out.println("1) Search for a keyword");
      System.out.println("2) Search for a phrase");
      System.out.println("3) Exit");
      System.out.print("Please enter a option number: ");
      int option = in.nextInt();
      
      if (option == 1) {
        System.out.print("Enter word you would like to search: ");
        String key = in.next();
        
        //retreive all the IIElements assoicated with our word
        ArrayList<IIElement> word = invertedIndex.get(key);
        
        //incase the word does not appear in any of the books
        if (word == null) {
          System.out.println("Word could not be found");
          continue;
        }
        
        System.out.println("Here are the top 5 occurances of " + key);
        for (int i = 0; i < 5 && i < word.size(); i++) {
          IIElement e = word.get(i);
          System.out.println(e.getFilename() + " -> " + e.getFreq());
        }
        
      }
      else if (option == 2) {
        ;
      }
      else if (option == 3) {
        System.exit(1);
      }
      else {
        System.out.println("Invalid option. Please try again.");
      }
      
    }
  
  }
}