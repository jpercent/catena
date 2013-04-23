package syndeticlogic.catena.text;

import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.type.Codeable;
import syndeticlogic.catena.utility.Codec;

public class DocumentDescriptor implements Codeable {
    private String doc;
    private int docId;
    private int docPrefixId;

    public DocumentDescriptor() {
    }
    
    public DocumentDescriptor(String doc, int docId, int docPrefixId) {
        this.doc = doc;
        this.docId = docId;
        this.docPrefixId = docPrefixId;
    }

    public String getDoc() {
        return doc;
    }

    public int getDocId() {
        return docId;
    }
    
    public void setDoc(String doc) {
        this.doc = doc;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public int getDocPrefixId() {
        return docPrefixId;
    }

    public void setDocPrefixId(int docPrefixId) {
        this.docPrefixId = docPrefixId;
    }

    @Override
    public int size() {
        return Type.INTEGER.length()+Type.STRING.length()+doc.length()+Type.INTEGER.length();
    }

    @Override
    public String oridinal() {
        throw new RuntimeException("Unsupported");
    }

    @Override
    public int encode(byte[] dest, int offset) {
        int copied = 0;
        copied += Codec.getCodec().encode(doc, dest, offset);
        copied += Codec.getCodec().encode(docId, dest, offset+copied); 
        copied += Codec.getCodec().encode(docPrefixId, dest, offset+copied); 
        return copied;
    }

    @Override
    public int decode(byte[] source, int offset) {
        int decoded = 0;
        doc = Codec.getCodec().decodeString(source, offset);
        decoded += Type.STRING.length()+doc.length();
        docId = Codec.getCodec().decodeInteger(source, offset+decoded);
        decoded += Type.INTEGER.length();
        docPrefixId = Codec.getCodec().decodeInteger(source, offset+decoded);
        decoded += Type.INTEGER.length();
        return decoded;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((doc == null) ? 0 : doc.hashCode());
        result = prime * result + docId;
        result = prime * result + docPrefixId;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DocumentDescriptor other = (DocumentDescriptor) obj;
        if (doc == null) {
            if (other.doc != null)
                return false;
        } else if (!doc.equals(other.doc))
            return false;
        if (docId != other.docId)
            return false;
        if (docPrefixId != other.docPrefixId)
            return false;
        return true;
    }

    @Override
    public int compareTo(Codeable c) {
        throw new RuntimeException("Unsupported");
    }
}