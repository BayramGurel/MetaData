import org.apache.tika.metadata.Metadata;

interface ICkanResourceFormatter {
    CkanResource format(String var1, Metadata var2, String var3, String var4);
}
