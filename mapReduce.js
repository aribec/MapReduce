/********************************************************
 * Step 1                                               *
 ********************************************************/

var m = function() {
  var yyyy = this.timestamp.getFullYear().toString();
  var mm = (this.timestamp.getMonth()+1).toString();
  var dd = this.timestamp.getDate().toString();
  emit( { user: this.user, date: yyyy + "-" + (mm[1]?mm:"0"+mm[0]) + "-" + (dd[1]?dd:"0"+dd[0]) },
        { products: [this.product] } );
};

var r = function(key, values) {
  var allProducts = [];
  for (var i = 0;i<values.length;i++){
        for(var j = 0;j<values[i]["products"].length;j++){
          var prod = values[i]["products"][j];
          allProducts.push(prod);
        }
  }

  // Es borren els elements duplicats de la col·leccio
  allProducts = allProducts.filter( function( item, index, inputArray ) {
           return inputArray.indexOf(item) == index;
  });

  // Es retorna la llista de productes
  // que pertanyen a un mateix usuari/dia
  return {products: allProducts};
};

db.runCommand( {
                 mapReduce: "newcollection",
                 map: m,
                 reduce: r,
                 out: {replace : "step1"}
               } );

/********************************************************
 * Step 2                                               *
 ********************************************************/

var m = function() {
  var products = this.value.products;
  
  for (var i=0;i<products.length;i++){
    for(var j=0;j<products.length;j++){
      if(products[i]!=products[j]){
        // a partir de totes variacions entre
        // els productes d'un usuari/dia s'emet
        // 1 punt s'evita marcar punts per a la
        // combicació d'un producte amb si mateix
        emit({product: products[i],recommended: products[j]},1);
      }
    }
  }
};

var r = function(key, values) {
  // Aquest reduce només cal que retorni
  // la suma de tots els valors 1 de cada
  // producte
  return Array.sum(values);
};


db.runCommand( {
                 mapReduce: "step1",
                 map: m,
                 reduce: r,
                 out: {replace : "step2"}
               } );



/********************************************************
 * Step 3                                               *
 ********************************************************/

var m = function() {
  var prodRec = this._id;
  // S'emet el producte amb la
  // seva recomanació i valor de puntuació
  emit(prodRec.product,{recommendations: [{product: prodRec.recommended, count: this.value}]});
};

var r = function(key, values) {
  recommendations = [];
  values.forEach( function(v) { recommendations = recommendations.concat(v.recommendations) } );
  recommendations.sort( function(a,b) { return b.count - a.count; } );
  return { recommendations: recommendations };

};

var f = function(key, value) {
  return { recommendations: value.recommendations.slice(0,10) };
}

db.runCommand( {
                 mapReduce: "step2",
                 map: m,
                 reduce: r,
                 finalize: f,
                 out: {replace : "recommendations"}
               } );

