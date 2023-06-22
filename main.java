import org.example.Variant;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Main {
    public static void main(String[] args) {
        // Path to the XML file
        String xmlFilePath = "path/to/your/xml/file.xml";

        try {
            // Parse the variants
            List<Variant> variants = parseVariants(xmlFilePath);

            // Process the variants and generate output
            processVariants(variants);
        } catch (IOException | XMLStreamException e) {
            e.printStackTrace();
        }
    }

  public class XMLParser {
      private String xmlFilePath;
      private String outputFile;
      private Set<Variant> variants;

      public XMLParser(String xmlFilePath, String outputFile) {
          this.xmlFilePath = xmlFilePath;
          this.outputFile = outputFile;
          this.variants = ConcurrentHashMap.newKeySet();
      }

      public void parseVariants() throws IOException, XMLStreamException {
          System.setProperty("entityExpansionLimit", "0");
          System.setProperty("totalEntitySizeLimit", "0");
          System.setProperty("jdk.xml.totalEntitySizeLimit", "0");

          Set<String> variantIdentifiers = new HashSet<>();
          Map<String, String> omimIdMap = new HashMap<>();

          try (FileInputStream inputStream = new FileInputStream(xmlFilePath)) {
              XMLInputFactory factory = XMLInputFactory.newFactory();
              XMLStreamReader reader = factory.createXMLStreamReader(inputStream);

              boolean inSequenceLocation = false;

              while (reader.hasNext()) {
                  int event = reader.next();

                  switch (event) {
                      case XMLStreamConstants.START_ELEMENT:
                          String elementName = reader.getLocalName();

                          if (elementName.equals("SequenceLocation")) {
                              inSequenceLocation = true;

                              String currentChromosome = reader.getAttributeValue(null, "Chr");
                              int currentStart = Integer.parseInt(reader.getAttributeValue(null, "start"));

                              String variantIdentifier = currentChromosome + "_" + currentStart;

                              if (!variantIdentifiers.contains(variantIdentifier)) {
                                  variantIdentifiers.add(variantIdentifier);

                                  Variant variant = new Variant();
                                  variant.setChromosome(currentChromosome);
                                  variant.setPosition(currentStart);

                                  variants.add(variant);
                              }
                          } else if (elementName.equals("XRef")) {
                              String db = reader.getAttributeValue(null, "DB");
                              String type = reader.getAttributeValue(null, "Type");
                              String id = reader.getAttributeValue(null, "ID");

                              if (db.equals("OMIM") && type.equals("MIM")) {
                                  omimIdMap.put(id, variantIdentifiers.iterator().next());
                              }
                          }

                          break;

                      case XMLStreamConstants.END_ELEMENT:
                          if (reader.getLocalName().equals("SequenceLocation")) {
                              inSequenceLocation = false;
                          }

                          break;
                  }
              }
          }

          processVariants(omimIdMap);
      }

      private void processVariants(Map<String, String> omimIdMap) {
          ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

          for (Variant variant : variants) {
              executor.submit(() -> {
                  String variantIdentifier = variant.getChromosome() + "_" + variant.getPosition();
                  String omimId = omimIdMap.getOrDefault(variantIdentifier, "null");
                  variant.setOmimId(omimId);
              });
          }

          executor.shutdown();

          while (!executor.isTerminated()) {
              // Wait for all tasks to complete
          }

          writeOutput();
      }

      private void writeOutput() throws IOException {
          PrintWriter writer = new PrintWriter(new FileWriter(outputFile));
          writer.println("chr\tstart\talleleID\tRef.\tAlt.\tOMIM ID");

          for (Variant variant : variants) {
              writer.println(variant.getChromosome() + "\t" + variant.getPosition() + "\t" +
                      variant.getAlleleID() + "\t" + variant.getRef() + "\t" + variant.getAlt() +
                      "\t" + variant.getOmimId());
          }

          writer.close();
      }

      public static void main(String[] args) {
          String xmlFilePath = "path/to/xml/file.xml";
          String outputFile = "path/to/output/file.txt";

          try {
              XMLParser parser = new XMLParser(xmlFilePath, outputFile);
              parser.parseVariants();
          } catch (IOException | XMLStreamException e) {
              e.printStackTrace();
          }
      }
  }
}
