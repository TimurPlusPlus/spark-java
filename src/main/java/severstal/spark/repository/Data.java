package severstal.spark.repository;

import java.io.Serializable;
import java.sql.Date;

public class Data implements Serializable {
    private long id;
    private long value;
    private Date date;

    public Data(long id, long value, Date date) {
        this.id = id;
        this.value = value;
        this.date = date;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
