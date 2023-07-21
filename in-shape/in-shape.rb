require 'bundler/inline'

gemfile do
  gem 'pry'
  gem 'bindata'
  gem 'rgeo'
  gem 'rgeo-shapefile'
  gem 'pg'
end
require 'pg'
require 'date'
require 'time'

shapefile = 'gis_osm_railways_07_1.shp'
database = 'test_postgis'
GEOM_COL_NAME = 'geom'
table_name = 'mygistable'
@conn = PG.connect(dbname: database, user: ENV['DB_USERNAME'], password: ENV['DB_PASSWORD'])

def valid_date_time?(datetime_str)
  begin
    dt = DateTime.parse(datetime_str)
    return dt.iso8601 
  rescue ArgumentError
    return false
  end
end
def convert_attr_to_sql(val)
  case val.class.to_s
  when 'Integer' then 'integer'
  when 'String' then 'text'
  when 'Float' then 'real'
  end
end
def ingest_attributes(file)
  first_row  = file.get(0)
  index      = first_row.index
  attributes = first_row.attributes
  geometry   = first_row.geometry

  new_attrs = attributes.map do |key, val|
    type = convert_attr_to_sql(val)
    "#{key.downcase} #{type}"
  end
  new_attrs << "#{GEOM_COL_NAME} GEOMETRY"
end

def create_table_from_shpfile(table_name, file)
  col_attrs = ingest_attributes(file)
  sql = "CREATE TABLE IF NOT EXISTS #{table_name} (id SERIAL PRIMARY KEY, #{col_attrs.join(', ')});"
  puts sql
  @conn.exec(sql)
  puts "Table created successfully"
end

RGeo::Shapefile::Reader.open(shapefile) do |file|
  table_name = shapefile.gsub('.shp','')
  puts "File contains #{file.num_records} records."
  if file.attributes_available?
    create_table_from_shpfile(table_name, file)
  end
    # Insert the records
  file.each do |record|
    attributes = record.attributes.map do |key, val|
      if val.class == String
        [ key, val.force_encoding("UTF-8") ]
      else
        [ key, val ]
      end
    end
    attributes << [GEOM_COL_NAME, record.geometry]
    # Convert the geometry to WKB
    # Construct the SQL INSERT INTO statement
    sql = "INSERT INTO #{table_name} (#{attributes.map(&:first).join(", ")}) VALUES (#{attributes.map(&:last).map.with_index {|x, i| "$#{i+1}"}.join(', ')});"
    puts sql
    params = attributes.map(&:last)
    @conn.exec(sql, params)


  end
  create_index_sql = "CREATE INDEX spatial_index_#{GEOM_COL_NAME}_on_#{table_name} ON #{table_name} USING gist (#{GEOM_COL_NAME})"
  @conn.exec(create_index_sql)
end
