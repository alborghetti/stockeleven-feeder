/*
 * Stock Eleven Firebase data feeder using google spreadsheet
 * Version 3 23.04.2016 - FALBORGH
 */

var firebase = require("firebase"),
    googlespreadsheet = require("google-spreadsheet"),
    CronJob = require('cron').CronJob,
    moment = require('moment-timezone');

var listRunner = function(oList, sListId, sTimezone) {
    console.log(sListId + ' started at: ' + Date());
    oList.getRows(1, function(err, row_data) {
        console.log(sListId + ' pulled in ' + row_data.length + ' rows');
        var aListStocks = [];
        var aStocks = [];
        var timestamp = Date.now();

        //Sort the list by final rank
        row_data.sort(function(a, b) {
            try {
                return parseFloat(a.finalrank) - parseFloat(b.finalrank);
            } catch (e) {
                return -1;
            }
        });

        for (var i = 0; i < row_data.length; i++) {
            //Set stocks for daily list
            if (i < 30) {
                aListStocks.push({
                    symbol: row_data[i].symbol,
                    company: row_data[i].company,
                    titoloTicker: row_data[i].titoloticker,
                    stock: row_data[i].stock,
                    lastPrice: row_data[i].lastprice,
                    dailyVariationP: row_data[i].dailyvariationp,
                    markCapMio: row_data[i].markcapmio,
                    pe: row_data[i].pe,
                    eps: row_data[i].eps,
                    high52: row_data[i].high52,
                    pToMax: row_data[i].ptomax,
                    distanceRank: parseInt(row_data[i].distancerank),
                    epsRank: parseInt(row_data[i].epsrank),
                    peRank: parseInt(row_data[i].perank),
                    sumRank: parseInt(row_data[i].sumrank),
                    finalRank: parseInt(row_data[i].finalrank)
                });
            }


            //Set global stocks info
            aStocks.push({
                symbol: row_data[i].symbol,
                company: row_data[i].company,
                titoloTicker: row_data[i].titoloticker,
                stock: row_data[i].stock,
                lastPrice: row_data[i].lastprice,
                dailyVariationP: row_data[i].dailyvariationp,
                markCapMio: row_data[i].markcapmio,
                pe: row_data[i].pe,
                eps: row_data[i].eps,
                high52: row_data[i].high52,
                pToMax: row_data[i].ptomax,
                distanceRank: parseInt(row_data[i].distancerank),
                epsRank: parseInt(row_data[i].epsrank),
                peRank: parseInt(row_data[i].perank),
                sumRank: parseInt(row_data[i].sumrank),
                finalRank: parseInt(row_data[i].finalrank)
            });
        }

        //Autenticate to Firebase and save the list
        var ref = new Firebase("https://stockeleven.firebaseio.com/");

        ref.authWithPassword({
            email: process.argv[2],
            password: process.argv[3]
        }, function(error, authData) {
            if (error) {
                console.log('Autentication error: ' + error.message);
            } else {
                console.log(sListId + ' loggedin');
                var list = ref.child(sListId + '/list');
                var previousLists = ref.child(sListId + '/previousLists');
                var stocks = ref.child(sListId + '/stocks');
                var stock = {};
                var stockId = "";
                var stockRank = 0;

                //Save current ranking list
                list.set({
                    timestamp: timestamp,
                    stocks: aListStocks
                }, function(error) {
                    if (error) {
                        console.log('Writing error: ' + error);
                    } else {
                        console.log(sListId + ' list saved');
                    }
                });

                //If it is friday we save the list in the history
                var day = moment.tz(timestamp, sTimezone).format('d');
                if (day >= 5) {
                    previousLists.push().set({
                        timestamp: timestamp,
                        stocks: aListStocks
                    }, function(error) {
                        if (error) {
                            console.log('Writing error: ' + error);
                        } else {
                            console.log(sListId + ' previous list saved');
                        }
                    });
                }

                //Update stocks data
                for (var i = 0; i < aStocks.length; i++) {
                    stock = aStocks[i];
                    stockId = aStocks[i].stock;
                    stockRank = aStocks[i].finalRank;
                    for (var property in stock) {
                        stocks.child('/' + stockId + '/' + property).set(stock[property]);
                    }


                    if (stockRank < 30) {
                        //Set if the stock is in the list
                        stocks.child('/' + stockId + '/inList').set(true);
                        stocks.child('/' + stockId + '/inListSince').transaction(function(currentData) {
                            if (currentData === null) {
                                return timestamp;
                            }
                        }, function(error, committed, snapshot) {
                            if (error) {
                                console.log(sListId + '-' + stockId + 'Transaction failed abnormally:', error);
                            }
                        });
                    } else {
                        stocks.child('/' + stockId + '/inList').set(false);
                    }
                }
            }
        });
    });
};

var cronTime = '00 00 20 * * 1-5'; //Start all work days at 20
//Start FTSE MIB
new CronJob({
    cronTime: cronTime,
    onTick: function() { listRunner(new googlespreadsheet('1-4h4xcdQ6T3ZYtiR4a6I3X7D12BZoFJmGVhdp9q52Ms'), 'L01', 'Europe/Rome') },
    start: true,
    timeZone: 'Europe/Rome'
});

//Start Nasdaq
new CronJob({
    cronTime: cronTime,
    onTick: function() { listRunner(new googlespreadsheet('1ai6mcXSF_tLnnLdiZ2FTFMh5nC7yWGBZpAcAGHdqczk'), 'L02', 'America/New_York') },
    start: true,
    timeZone: 'America/New_York'
});

cronTime = '00 05 20 * * 1-5'; //Start all work days at 20:05
//Start NYSE
new CronJob({
    cronTime: cronTime,
    onTick: function() { listRunner(new googlespreadsheet('1aOfxSRtlGMpu3x6IoKVIKGfaswpysU9VnZrSsElf6nc'), 'L03', 'America/New_York') },
    start: true,
    timeZone: 'America/New_York'
});

//Start Euronext Paris
new CronJob({
    cronTime: cronTime,
    onTick: function() { listRunner(new googlespreadsheet('1PuKvIJXSSjsORes7OkHNNT5QDR4QF5WrZupE8IPc6T4'), 'L04', 'Europe/Paris') },
    start: true,
    timeZone: 'Europe/Paris'
});

cronTime = '00 10 20 * * 1-5'; //Start all work days at 20:10
//Start DAX Frankfurt
new CronJob({
    cronTime: cronTime,
    onTick: function() { listRunner(new googlespreadsheet('1hefXU2-32hzXh9aThc-6gvlr67PJDU8FTtcf0Yov33A'), 'L05', 'Europe/Berlin') },
    start: true,
    timeZone: 'Europe/Berlin'
});

//Start FTSE London
new CronJob({
    cronTime: cronTime,
    onTick: function() { listRunner(new googlespreadsheet('1eePLRVHF6rsEiJSpZ-ApSmkr3MiEl7qwHlamraJtuEw'), 'L06', 'Europe/London') },
    start: true,
    timeZone: 'Europe/London'
});