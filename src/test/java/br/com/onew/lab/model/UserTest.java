package br.com.onew.lab.model;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

public class UserTest extends User {

  @Test
  public void testCreateUsers() {
    User user = new User();
    user.setName("Diego");
    user.setFavoriteNumber(1243);
    user.setFavoriteColor("Blue");
    
    assertEquals(user.getName(), "Diego");
    assertEquals(user.getFavoriteNumber(), new Integer(1243));
    assertEquals(user.getFavoriteColor(), "Blue");
  }
  
  @Test 
  public void testObrigatoryFieldNull() {
    User user = new User();
    user.setName(null);
  }
  
  @Test
  public void testSerialize() {
    DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
    User user = new User("Diego", 1243, "Blue");
    DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
    try {
      dataFileWriter.create(user.getSchema(), new File("user.avro"));
      dataFileWriter.append(user);
      dataFileWriter.close();
      
    } catch (IOException ex) {
      fail(ex.getMessage());
    }
  }
  
  @Test
  public void testDeserialize() {
    DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
    try {
      DataFileReader<User> dataFileReader = new DataFileReader<User>(new File("user.avro"), userDatumReader);
      assertTrue(dataFileReader.hasNext());
      User user = dataFileReader.next();
      assertEquals(user.getName().toString(), "Diego");
      assertEquals(user.getFavoriteNumber(), new Integer(1243));
      assertEquals(user.getFavoriteColor().toString(), "Blue");
      dataFileReader.close();
    } catch (IOException ex) {
      fail(ex.getMessage());
    }
    
  }
  
  

}
