/*
 * Stock Eleven Firebase data feeder for portfolios using google spreadsheet
 * Version 1 21.04.2016 - FALBORGH
 */

var firebase = require("firebase"),
    googlespreadsheet = require("google-spreadsheet"),
    P01Base = //FTSE MIB
    new googlespreadsheet('1ZnJQZq_9ebFt0qFLJSKtTLZCiSwQMgGR30PklFrkMgE'),
    P02Base = //Nasdaq
    new googlespreadsheet('1gszaQttfNyTAvkTTJS5nZ-pTMc3hiTRXEWN1AAwrFKs'),
    P03Base = //NYSE
    new googlespreadsheet('1tZTeS0GTTfM7_b2MoVIEB1XKFVcRQK7I-Un6yYcBYuA'),
    P04Base = //Euronext Paris
    new googlespreadsheet('1AmUD1aBbqoGn4L7LnH4L2lAbXq54axEvUEo50gTraeM'),
    P05Base = //DAX Frankfurt
    new googlespreadsheet('1MhDCIi_TQcSOGjMFiVjj3_JrXZR4bVw_lCZZHoPwjYA'),
    P06Base = //FTSE London
    new googlespreadsheet('1XSYxC6O5kPBohk9msnESl-sOB_BjRvblsmlA5BAoxrU'),
    scheduler = require('cron').CronJob;

var listRunner = {

        start: function(oPortfolio, sPortfolioId, sTimeZone) {
            new scheduler(
                '00 00 22 * * 1-5',
                function() {
                    /*
                     * Runs every weekday (Monday through Friday)
                     * at 22:00:00 of specified timezone.
                     * It does not run on Saturday or Sunday.
                     */
                    console.log(sPortfolioId + ' started at: ' + Date());
                    oPortfolio.getRows(1, function(err, row_data) {

                        console.log(sPortfolioId + ' pulled in ' + row_data.length + ' rows');

                        var aPortfolioStocks = [];
                        var oPortfolioValue = {};
                        var oMarketValue = {};
                        var timestamp = Date.now();
                        var sDifference = "";

                        //Sort the portfolios by company
                        row_data.sort(function(a, b) {
                            if (a.company < b.company) return -1;
                            if (a.company > b.company) return 1;
                            return 0;
                        });

                        for (var i = 0; i < row_data.length; i++) {

                            if (row_data[i].company === 'ZZ_PORTFOLIO') {
                                oPortfolioValue = {
                                    purchaseValue: row_data[i].purchasevalue,
                                    currentValue: row_data[i].currentvalue,
                                    pl: row_data[i].pl,
                                    deltaP: row_data[i].deltap,
                                    purchasedOn: row_data[i].purchasedon,
                                    plannedSell: row_data[i].plannedsell
                                }
                            } else if (row_data[i].company === 'ZZ_MARKET') {
                                oMarketValue = {
                                    lastPrice: row_data[i].lastprice,
                                    purchasePrice: row_data[i].purchaseprice,
                                    quantity: row_data[i].quantity,
                                    purchaseValue: row_data[i].purchasevalue,
                                    currentValue: row_data[i].currentvalue,
                                    pl: row_data[i].pl,
                                    deltaP: row_data[i].deltap
                                }
                            } else if (row_data[i].company === 'ZZ_DIFFERENCE') {
                                sDifference = row_data[i].deltap;
                            } else {
                                aPortfolioStocks.push({
                                    company: row_data[i].company,
                                    stock: row_data[i].stock,
                                    lastPrice: row_data[i].lastprice,
                                    dailyVariationP: row_data[i].dailyvariationp,
                                    purchasePrice: row_data[i].purchaseprice,
                                    quantity: row_data[i].quantity,
                                    purchaseValue: row_data[i].purchasevalue,
                                    currentValue: row_data[i].currentvalue,
                                    pl: row_data[i].pl,
                                    deltaP: row_data[i].deltap,
                                    purchasedOn: row_data[i].purchasedon,
                                    plannedSell: row_data[i].plannedsell
                                });
                            }
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
                                ref.child(sPortfolioId + '/timestamp').set(timestamp);

                                ref.child(sPortfolioId + '/stocks').set(aPortfolioStocks);

                                ref.child(sPortfolioId + '/value').set(oPortfolioValue);
                                ref.child(sPortfolioId + '/previousValues/' + timestamp).child('/value').set(oPortfolioValue);

                                ref.child(sPortfolioId + '/marketValue').set(oMarketValue);
                                ref.child(sPortfolioId + '/previousMarketValues/' + timestamp).child('/marketValue').set(oMarketValue);

                                ref.child(sPortfolioId + '/difference').set(sDifference);
                                ref.child(sPortfolioId + '/previousDifferences/' + timestamp).child('/difference').set(sDifference);

                                console.log('Portfolio ' + sPortfolioId + ' saved');
                            }
                        });
                    });
                },
                null,
                true,
                sTimeZone
            );

            start: function(oPortfolio, sPortfolioId, sTimeZone) {
                new scheduler('00 00 22 * * 1-5', function() {
                        /*
                         * Runs every weekday (Monday through Friday)
                         * at 22:00:00 of specified timezone.
                         * It does not run on Saturday or Sunday.
                         */
                        oPortfolio.getRows(1, function(err, row_data) {

                            console.log(sPortfolioId + ' pulled in ' + row_data.length + ' rows');

                            var aPortfolioStocks = [];
                            var oPortfolioValue = {};
                            var oMarketValue = {};
                            var timestamp = Date.now();
                            var sDifference = "";

                            //Sort the portfolios by company
                            row_data.sort(function(a, b) {
                                if (a.company < b.company) return -1;
                                if (a.company > b.company) return 1;
                                return 0;
                            });

                            for (var i = 0; i < row_data.length; i++) {

                                if (row_data[i].company === 'ZZ_PORTFOLIO') {
                                    oPortfolioValue = {
                                        purchaseValue: row_data[i].purchasevalue,
                                        currentValue: row_data[i].currentvalue,
                                        pl: row_data[i].pl,
                                        deltaP: row_data[i].deltap,
                                        purchasedOn: row_data[i].purchasedon,
                                        plannedSell: row_data[i].plannedsell
                                    }
                                } else if (row_data[i].company === 'ZZ_MARKET') {
                                    oMarketValue = {
                                        lastPrice: row_data[i].lastprice,
                                        purchasePrice: row_data[i].purchaseprice,
                                        quantity: row_data[i].quantity,
                                        purchaseValue: row_data[i].purchasevalue,
                                        currentValue: row_data[i].currentvalue,
                                        pl: row_data[i].pl,
                                        deltaP: row_data[i].deltap
                                    }
                                } else if (row_data[i].company === 'ZZ_DIFFERENCE') {
                                    sDifference = row_data[i].deltap;
                                } else {
                                    aPortfolioStocks.push({
                                        company: row_data[i].company,
                                        stock: row_data[i].stock,
                                        lastPrice: row_data[i].lastprice,
                                        dailyVariationP: row_data[i].dailyvariationp,
                                        purchasePrice: row_data[i].purchaseprice,
                                        quantity: row_data[i].quantity,
                                        purchaseValue: row_data[i].purchasevalue,
                                        currentValue: row_data[i].currentvalue,
                                        pl: row_data[i].pl,
                                        deltaP: row_data[i].deltap,
                                        purchasedOn: row_data[i].purchasedon,
                                        plannedSell: row_data[i].plannedsell
                                    });
                                }
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
                                    ref.child(sPortfolioId + '/timestamp').set(timestamp);

                                    ref.child(sPortfolioId + '/stocks').set(aPortfolioStocks);

                                    ref.child(sPortfolioId + '/value').set(oPortfolioValue);
                                    ref.child(sPortfolioId + '/previousValues/' + timestamp).child('/value').set(oPortfolioValue);

                                    ref.child(sPortfolioId + '/marketValue').set(oMarketValue);
                                    ref.child(sPortfolioId + '/previousMarketValues/' + timestamp).child('/marketValue').set(oMarketValue);

                                    ref.child(sPortfolioId + '/difference').set(sDifference);
                                    ref.child(sPortfolioId + '/previousDifferences/' + timestamp).child('/difference').set(sDifference);

                                    console.log('Portfolio ' + sPortfolioId + ' saved');
                                }
                            });
                            //Logout
                            ref.unauth();
                        });
                    },
                    null,
                    true,
                    sTimeZone
                );
            }
        };

        //Start FTSE MIB Base portfolio
        listRunner.start(P01Base, "P01Base", "Europe/Rome");
        //Start Nasdaq Base portfolio
        listRunner.start(P02Base, "P02Base", "America/New_York");
        //Start NYSE Base portfolio
        listRunner.start(P03Base, "P03Base", "America/New_York");
        //Start Euronext Paris
        listRunner.start(P04Base, "P04Base", "Europe/Paris");
        //Start DAX Frankfurt
        listRunner.start(P05Base, "P05Base", "Europe/Berlin");
        //Start FTSE London
        listRunner.start(P06Base, "P06Base", "Europe/London");

        console.log('Jobs started');