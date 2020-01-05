package org.hasadna.gtfs.entity;

import javax.persistence.Entity;
import javax.persistence.Id;
import java.util.Objects;

@Entity(name="key_value")
public class KeyValue {

    @Id
    private String keyed;

    private String kind ;   // shape / siri / gtfs
    private String value;

    public KeyValue() {
    }

    public static KeyValue make(String kind, String key, String value) {
        KeyValue kv = new KeyValue();
        kv.setKind(kind);
        kv.setKeyed(key);
        kv.setValue(value);
        return kv;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public String getKeyed() {
        return keyed;
    }

    public void setKeyed(String keyed) {
        this.keyed = keyed;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "KeyValue{" +
                "keyed='" + keyed + '\'' +
                ", kind='" + kind + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KeyValue)) return false;
        KeyValue keyValue = (KeyValue) o;
        return getKeyed().equals(keyValue.getKeyed()) &&
                getKind().equals(keyValue.getKind());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getKeyed(), getKind());
    }
}
