const {Storage} = require('@google-cloud/storage');
const csv = require('csv-parser');


exports.readObservation = (file, context) => {


    const gcs = new Storage();

    const datafile = gcs.bucket(file.bucket).file(file.name);


    datafile.createReadStream()
        .on('error', () => {
            // handle an error
            console.error(error);
        })
        .pipe(csv())
        .on('data', (row) => {
            // log raw data
            //console.log(row)
            printDict(row, file.name);

        })
        .on('end', () => {
            // handle end of csv
            console.log('end!');
        })

}

// HELPER FUNCTION
function printDict(row, fileName) {
  // for each value in row object, if the value is -9999 then replace it with null
  for (let key in row) {
      if (key === 'station') {
          row[key] = fileName.split('.')[0];
      } else {
              row[key] = parseFloat(row[key]);
              if (row[key] === -9999) {
                  row[key] = null;
              } else {
                  if (key === 'airtemp' || key === 'dewpoint' || key === 'pressure' || key === 'windspeed' || key === 'precip1hour' || key === 'precip6hour') {
                      row[key] = row[key] / 10;
                  }
              }
      }
      console.log(row);
  }
  writetoBq(row)
}

const {BigQuery} = require('@google-cloud/bigquery');

const bq = new BigQuery();
const datasetid = 'weather_etl';
const tableid = 'weather_etl_assignment';

// define an entry point function
// const democode = () => {
//     //create a fake object
//     fakeobject = {};
//     fakeobject.first_name = "kiran";
//     fakeobject.last_name = "bhosale";
//     fakeobject.email = "kbhosale@iu.edu";
//     fakeobject.age = 23;
//     writetoBq(fakeobject);
//
// }

// call that entry point function
// democode();

// create a helper function that writes to bq
// function must be asynchronuous
async function writetoBq(obj) {
    //bq expect an array of objects , but this function only receives 1
    var rows = []; // empty array
    rows.push(obj);

    // insert array of objects into the demo table
    await bq
        .dataset(datasetid)
        .table(tableid)
        .insert(rows)
        .then(() => {
            rows.forEach((row) => {
                console.log(`inserted: ${row}`)
            })
        })
        .catch((err) => {
          console.error('Partial failure occurred:', err.errors);
          err.errors.forEach((error) => {
            // Log the detailed information of each error
            console.error('Error:', error);
          });
        })


}


// Alternate implementation
// const writetoBq = (obj) => {};




