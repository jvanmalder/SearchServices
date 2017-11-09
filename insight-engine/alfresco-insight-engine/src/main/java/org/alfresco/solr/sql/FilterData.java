/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.alfresco.solr.sql;

import java.util.*;

class FilterData {

    private Map<String, Filter> filters = new HashMap();

    public FilterData() {

    }

    public FilterData(String s) {
        if(s != null & s.length() > 0) {
            String[] parts = s.split("~:~");
            for (String part : parts) {
                String[] entry = part.split("=:=");
                String key = entry[0];
                String value = entry[1];
                filters.put(key, new Filter(value));
            }
        }
    }

    public Filter getFilter(String key) {
        return filters.get(key);
    }

    public void addStart(String key, String start, boolean inclusive) {
        if(filters.containsKey(key)) {
            Filter f = filters.get(key);
            f.setStart(start, inclusive);
        } else {
            Filter f = new Filter();
            f.setStart(start, inclusive);
            filters.put(key, f);
        }
    }

    public void addEnd(String key, String end, boolean inclusive) {
        if(filters.containsKey(key)) {
            Filter f = filters.get(key);
            f.setEnd(end, inclusive);
        } else {
            Filter f = new Filter();
            f.setEnd(end, inclusive);
            filters.put(key, f);
        }
    }

    public String toString() {
        Set<Map.Entry<String, Filter>> entries = filters.entrySet();

        StringBuilder builder = new StringBuilder();
        for(Map.Entry<String, Filter> entry : entries) {
            if(builder.length() > 0) {
                builder.append("~:~");
            }
            builder.append(entry.getKey()+"=:="+entry.getValue().toString());
        }

        return builder.toString();
    }

    public static class Filter {
        private String start;
        private String end;
        private boolean inclusiveStart = true;
        private boolean inclusiveEnd = true;

        public Filter(String f) {
            String[] parts = f.split("~");
            if (!parts[0].equals("i"))
            {
                inclusiveStart = false;
            }

            if (!parts[1].equals("*"))
            {
                start = parts[1];
            }

            if (!parts[2].equals("i"))
            {
                inclusiveEnd = false;
            }

            if (!parts[3].equals("*"))
            {
                end = parts[3];
            }
        }

        public Filter() {

        }

        public void setStart(String start, boolean inclusive) {
            this.start = start;
            this.inclusiveStart = inclusive;
        }

        public void setEnd(String end, boolean inclusive) {
            this.end = end;
            this.inclusiveEnd = inclusive;
        }

        public String getStart() {
            return this.start;
        }

        public String getEnd() {
            return this.end;
        }

        public String toString() {
            StringBuilder buf = new StringBuilder();
            if(inclusiveStart) {
                buf.append("i~");
            } else {
                buf.append("e~");
            }

            if(start != null) {
                buf.append(start+"~");
            } else {
                buf.append("*~");
            }

            if(inclusiveEnd) {
                buf.append("i~");
            } else {
                buf.append("e~");
            }

            if(end != null) {
                buf.append(end);
            } else {
                buf.append("*");
            }

            return buf.toString();
        }
    }
}
