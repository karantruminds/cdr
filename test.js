const fs = require("fs");
const Pool = require("pg").Pool;
const fastcsv = require("fast-csv");

let stream = fs.createReadStream("Data.csv");
let csvData = [];
let csvStream = fastcsv
  .parse()
  .on("data", function(data) {
    csvData.push(data);
  })
  .on("end", function() {

    //csvData.shift();

    const pool = new Pool({
      host: "localhost",
      user: "postgres",
      database: "postgres",
      password: "ujjwal1999",
      port: 5432
    });

    

    const query ="INSERT INTO tutu1(date, sub_site_id, sub_site,mb_used,gb) VALUES($1, $2, $3, $4, $5)";
    const query1 ="create table tutu1(date date,sub_site_id integer,sub_site varchar(50),mb_used decimal,gb decimal)";
    const query2 ="drop table tutu1";

    //console.log(csvData)

    pool.connect((err, client, done) => {
      if (err) throw err;
        try{
        client.query(query2,(err, res) => {
            if (err) {console.log(err.stack);} 
        });
        client.query(query1,(err, res) => {
            if (err) {console.log(err.stack);} 
        });
    const index_date=csvData[0].indexOf("Date");
    const index_subsiteid=csvData[0].indexOf("Sub Site ID");
    const index_subsite=csvData[0].indexOf("Sub Site");
    const index_mb=csvData[0].indexOf("MB Used");
    const index_gb=csvData[0].indexOf("GB");

    //console.log(csvData[0]);
        
        
        for(var i=1;i<csvData.length-1;i++){
                var row=csvData[i];
                let date1 = row[index_date];
                let sub_site_id1 = row[index_subsiteid];
                let sub_site1 = row[index_subsite];
                let mb_used1 = row[index_mb];
                let gb1 = row[index_gb];

          client.query(query,  [date1, sub_site_id1, sub_site1,mb_used1,gb1], (err, res) => {
            if (err) {console.log(err.stack);} 
            else {
              //console.log("inserted " + res.rowCount + " row:", row);
            }
          });
          
        };
      
        
        client.query(`DROP TABLE res`,(err,res)=>{
            if (err) {console.log(err.stack);}
        });
        
        client.query(`CREATE TABLE res("date","sub_site",sum) AS SELECT "date","sub_site",SUM("mb_used") FROM tutu1 GROUP BY "sub_site","date" `,(err,res)=>{
            if (err) {console.log(err.stack);}
        });
        
        const prompt = require('prompt-sync')();
      const name = prompt('Enter the name ');
      const date = prompt('Enter the date ');
        query5 =`SELECT * from res where sub_site = '${name}' and date = '${date}'`;
        
        client.query(`SELECT * from res where sub_site = '${name}' and date = '${date}' `,(err,res)=>{
            if (err) {console.log(err.stack);}
            else {
                console.log(res.rows);
                done();
            }
        });
        
      }
        finally{
          //done();
        }
    });
    });

stream.pipe(csvStream);