
KNN_INDEX = {
    "settings": {
        "index.knn": True,
        "index.knn.space_type": "cosinesimil",
        "analysis": {
          "analyzer": {
            "default": {
              "type": "standard",
              "stopwords": "_english_"
            }
          }
        }
    },
    "mappings": {
        "properties": {
            "vector_field": {
                "type": "knn_vector",
                "dimension": 384
                
            },
            "text_field": {
                "type": "text"
                
            },
            "metadata": {
                "type": "object"
                
            }
        }
    }
}