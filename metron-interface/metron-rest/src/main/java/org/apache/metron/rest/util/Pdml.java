/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.metron.rest.util;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.ArrayList;
import java.util.List;
/**
 *
 * @author msumbul
 */
@JacksonXmlRootElement
public class Pdml {

  @JacksonXmlProperty(isAttribute = true)
  public String version;
  @JacksonXmlProperty(isAttribute = true)
  public String creator;
  @JacksonXmlProperty(isAttribute = true)
  public String time;
  @JacksonXmlProperty(isAttribute = true)
  public String capture_file;

  @JacksonXmlProperty(localName = "packet")
  @JacksonXmlElementWrapper(useWrapping = false)
  public List<Packet> packets;

  public void setPackets(List<Packet> value){
    if (packets == null){
      packets = new ArrayList<Packet>(value.size());
    }
    packets.addAll(value);
  }

  public static class Packet {

    @JacksonXmlProperty(localName = "proto")
    @JacksonXmlElementWrapper(useWrapping = false)
    public List<Proto> protos;

    public void setProtos(List<Proto> value){
      if (protos == null){
        protos = new ArrayList<Proto>(value.size());
      }
      protos.addAll(value);
    }
  }


  public static class Proto {

    @JacksonXmlProperty(isAttribute = true)
    public String name;
    @JacksonXmlProperty(isAttribute = true)
    public String pos;
    @JacksonXmlProperty(isAttribute = true)
    public String showname;
    @JacksonXmlProperty(isAttribute = true)
    public String size;
    @JacksonXmlProperty(isAttribute = true)
    public String hide;

    @JacksonXmlProperty(localName = "field")
    @JacksonXmlElementWrapper(useWrapping = false)
    public List<Field> fields;

    public void setFields(List<Field> value){
      if (fields == null){
        fields = new ArrayList<Field>(value.size());
      }
      fields.addAll(value);
    }
  }

  public static class Field {

    @JacksonXmlProperty(isAttribute = true)
    public String name;
    @JacksonXmlProperty(isAttribute = true)
    public String pos;
    @JacksonXmlProperty(isAttribute = true)
    public String showname;
    @JacksonXmlProperty(isAttribute = true)
    public String size;
    @JacksonXmlProperty(isAttribute = true)
    public String value;
    @JacksonXmlProperty(isAttribute = true)
    public String show;
    @JacksonXmlProperty(isAttribute = true)
    public String unmaskedvalue;
    @JacksonXmlProperty(isAttribute = true)
    public String hide;

    @JacksonXmlProperty(localName = "field")
    @JacksonXmlElementWrapper(useWrapping = false)
    public List<Field> fields;

    public void setFields(List<Field> value){
      if (fields == null){
        fields = new ArrayList<Field>(value.size());
      }
      fields.addAll(value);
    }

    @JacksonXmlProperty(localName = "proto")
    @JacksonXmlElementWrapper(useWrapping = false)
    public List<Proto> protos;

    public void setProtos(List<Proto> value){
      if (protos == null){
        protos = new ArrayList<Proto>(value.size());
      }
      protos.addAll(value);
    }

  }
}
