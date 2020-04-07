curl -X DELETE "http://localhost:9200/covid19_raw_data?pretty"





curl -X PUT "http://localhost:9200/covid19_raw_data?pretty" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "Country":   { "type": "keyword"  },
	  "Province":   { "type": "keyword"  },
      "Location":    { "type": "geo_point", "ignore_malformed":"true" },
	  "Date":    { "type": "date", "format": "dd-MM-yyyy HH:mm:ss||dd-MM-yyyy" },  
      "Cases":    { "type": "float" },
	  "Status":    { "type": "keyword" }
    }
  }
}
'

#ELASTICSEARCH SERVERS
["ec2-13-58-53-1.us-east-2.compute.amazonaws.com","ec2-3-135-203-125.us-east-2.compute.amazonaws.com","ec2-18-220-232-235.us-east-2.compute.amazonaws.com"]

#delete index
curl -u "elastic:W|Hed%/E$]=(" -X DELETE "http://ec2-13-58-53-1.us-east-2.compute.amazonaws.com:9200/covid19_prediction_data?pretty"



#delete data by query
curl -u "elastic:W|Hed%/E$]=(" -X POST "http://ec2-18-220-232-235.us-east-2.compute.amazonaws.com:9200/covid19_raw_data/_delete_by_query" -H 'Content-Type: application/json' -d'
{
"query": {
"range" : {
"Date" : {
"gte" : "04/04/2020",
"format": "dd/MM/yyyy"
}
}
}
}'


#search data by query
curl -u "elastic:W|Hed%/E$]=(" -X GET "http://ec2-13-58-53-1.us-east-2.compute.amazonaws.com:9200/covid19_raw_data/_search" -H 'Content-Type: application/json' -d'
{
"query": {
"range" : {
"Date" : {
"gte" : "04/04/2020",
"format": "dd/MM/yyyy"
}
}
}
}'





curl -u "remote_monitoring_user:d>:F-4?lK>6." -X GET "http://ec2-13-58-53-1.us-east-2.compute.amazonaws.com:9200"

#elasticsearch-kibana-errors-data-too-large
curl -u "elastic:W|Hed%/E$]=(" -X POST "http://ec2-13-58-53-1.us-east-2.compute.amazonaws.com:9200/covid19_raw_data/_cache/clear"
#or increase jvm memory settings


curl -u "elastic:W|Hed%/E$]=(" -X PUT "http://ec2-13-58-53-1.us-east-2.compute.amazonaws.com:9200/covid19_raw_data?pretty" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "Country":   { "type": "keyword"  },
      "CountryCode":   { "type": "keyword"  },
	  "Province":   { "type": "keyword"  },
      "City":   { "type": "keyword"  },
      "CityCode":   { "type": "keyword"  },
      "Location":    { "type": "geo_point", "ignore_malformed":"true" },
	  "Confirmed":    { "type": "integer" },
      "Deaths":    { "type": "integer" },
      "Recovered":    { "type": "integer" },
      "Active":    { "type": "integer" },
      "Date":    { "type": "date", "format": "dd-MM-yyyy HH:mm:ss||dd-MM-yyyy" }
    }
  }
}
'

curl -u "elastic:W|Hed%/E$]=(" -X POST "http://ec2-13-58-53-1.us-east-2.compute.amazonaws.com:9200/_template/covid19_raw_data" -H 'Content-Type: application/json' -d'
{
  "index_patterns": ["*"],
  "order": -1,
  "settings": {
    "number_of_shards": "6",
    "number_of_replicas": "3"
  }
}
'

#Scripted_field :  country_name
if(doc['Country'].size() != 0){
    if(doc['Country'].value== "US"){
     return "United States";
    }
    else if(doc['Country'].value== "Congo (Brazzaville)"){
     return "Congo";
    }
    else if(doc['Country'].value== "Congo (Kinshasa)"){
     return "Democratic Republic of the Congo";
    }
    else if(doc['Country'].value== "Libya"){
     return "Libyan Arab Jamahiriya";
    }
    else if(doc['Country'].value== "Tanzania"){
     return "United Republic of Tanzania";
    }
    else if(doc['Country'].value== "Syria"){
     return "Syrian Arab Republic ";
    }
    else if(doc['Country'].value== "Czechia"){
     return "Czech Republic";
    }
    else{
        return doc['Country'].value;
    }
}



##PREDICTION INDEX

curl -u "elastic:W|Hed%/E$]=(" -X PUT "http://ec2-13-58-53-1.us-east-2.compute.amazonaws.com:9200/covid19_prediction_data?pretty" -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "Country":   { "type": "keyword"  },
	  "Date":    { "type": "date", "format": "dd-MM-yyyy HH:mm:ss||dd-MM-yyyy" },  
      "Confirmed_Cases":    { "type": "integer" },
      "Predicted_Confirmed_Cases":    { "type": "integer" },
      "Recovered_Cases":    { "type": "integer" },
      "Predicted_Recovered_Cases":    { "type": "integer" },
	  "Dead_Cases":    { "type": "integer" },
      "Predicted_Dead_Cases":    { "type": "integer" }
    }
  }
}
'

curl -u "elastic:W|Hed%/E$]=(" -X DELETE "http://ec2-13-58-53-1.us-east-2.compute.amazonaws.com:9200/covid19_prediction_data?pretty"

