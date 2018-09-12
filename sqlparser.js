const fs = require('fs');
const fse = require('fs-extra');
const sql = require('mssql');
var moment = require('moment');
var MongoClient = require('mongodb').MongoClient

MongoClient.connect("mongodb://userReadWrite:Pr3d1CtR0niC$#Inc@localhost:8099/ShanhuTest", async (err, db) => {
    var MSSQLtoCSV = async (FileDataPath) => {
        let SerialNumberLUTincomplete = db.collection('SerialNumberLUTincomplete')
        let SerialNumberLUTcompleted = db.collection('SerialNumberLUTcompleted')
        let SerialNumberLUTdiscarded = db.collection('SerialNumberLUTdiscarded')
        let collection = db.collection("PdxUser")

        //File name related parameters
        var Timestamp = moment(new Date()).format('YYYYMMDD')
        var FileTimestamp

        //Parameters for lastruntime and last latest106T116Row
        var lastRunTime = new Date()
        var latest106T116Row = ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,4201264630,0.23,46.00,0,0.00,45.83,23.35,0,0,0,W11203637,,,,,,,,,,,,,,,,"
        var newlastRunTime = new Date()

        //Parsing parameters
        var TagRepeatCount
        var flag106T116
        var flag17T30
        var flagTag1


        await collection.find({
            "id": 95
        }, { 'lastRunTime': 1, 'latest106T116Row': 1, _id: 0 }).toArray(async (err, companyDetails) => {
            lastRunTime = companyDetails[0].lastRunTime
            latest106T116Row = companyDetails[0].latest106T116Row
            try {
                var pool = await sql.connect('Driver={SQL Server Native Client 11.0};Data Source={cvd-mesdbp1};User Id=na\\predcvd;password=gbYsARz13;Database={RoviSysClevelandIIoT};Integrated Security={true};Trusted_Connection={yes};')
                const request = new sql.Request()
                var result1
                sqlTimeStamp = "'" + moment(lastRunTime).format("YYYY-MM-DD HH:mm:ss.SS Z") + "'"
                console.log(sqlTimeStamp)
                result1 = await request.query("SELECT DISTINCT SerialNumberId, Min(TagTimestamp) FROM TagHistory where TagTimestamp > " + sqlTimeStamp + " GROUP BY SerialNumberId ORDER BY Min(TagTimestamp) ASC, SerialNumberId")
                for (var i = 0; i < result1.recordset.length; i++) {
                    var currentSerialId = result1.recordset[i].SerialNumberId
                    if (currentSerialId != null) {
                        await SerialNumberLUTcompleted.find({
                            SerialNumberId: currentSerialId
                        }).toArray(async (err, currentSerialIdDetails) => {


                        })
                        var result2 = await request.query('select PartId from SerialNumber where SerialNumberId=' + currentSerialId)
                        var result3 = await request.query("select distinct(TagTimestamp) from TagHistory where SerialNumberId=" + currentSerialId + " and TagTimestamp > " + sqlTimeStamp + " order by TagTimestamp ASC")
                        var data = ""
                        var partId, serialNumber, interval, rowCount
                        partId = result2.recordset[0] != undefined ? result2.recordset[0].PartId : "default"
                        var serialNumberStartTime = result3.recordset[0].TagTimestamp;
                        serialNumber = result1.recordset[i].SerialNumberId
                        rowCount = result3.recordset.length
                        var startTime = new Date(), endTime = new Date()
                        flag106T116 = false
                        flag17T30 = false
                        flagTag1 = false
                         if (Math.floor(((new Date()).getTime() - serialNumberStartTime.getTime()) / 1000) >= 900) {
                            //Removing the completed serial number from SerialNumberLUTincomplete collection
                            console.log(serialNumber)
                            SerialNumberLUTincomplete.remove({
                                SerialNumberId: serialNumber
                            }, function (err, result) {
                                if (err) {
                                    console.log(err)
                                }
                            })
                        if (result3.recordset.length != 0) {
                            FileTimestamp = moment(new Date(result3.recordset[0].TagTimestamp.toISOString())).format('YYYYMMDDHHmmss')
                            Timestamp = moment(new Date(result3.recordset[0].TagTimestamp.toISOString())).format('YYYYMMDD')
                         }
                        for (let j = 0; j < result3.recordset.length; j++) {
                            Timestamp = moment(new Date(result3.recordset[j].TagTimestamp.toISOString())).format('YYYYMMDD')
                            var result4 = await request.query(`SELECT a.TagId,a.TagValue FROM TagHistory a INNER JOIN (select MIN(TagHistoryId) as TagHistoryId,TagId from TagHistory where SerialNumberId=` + serialNumber + ` and TagTimest
amp= '` + result3.recordset[j].TagTimestamp.toISOString() + `' group by TagId) AS b ON a.TagHistoryId = b.TagHistoryId AND a.TagId = b.TagId  where a.TagTimestamp > ` + sqlTimeStamp + ` order by TagId ASC`)
                            var TagValue = []
                            var row = ""
                            result4.recordset.forEach((record, index) => {
                                TagValue[record["TagId"]] = record["TagValue"]
                            }
                            )

                            for (let i = 0; i < 133; i++) {
                                if (i == 0) {
                                    data = data + Math.floor(new Date(result3.recordset[j].TagTimestamp.toISOString()).getTime() - new Date(result3.recordset[0].TagTimestamp.toISOString()).getTime()) + ','
                                    row = row + ','
                                }
                                else {
                                    if (TagValue[i] == undefined) {
                                        data = data + "" + ','
                                        row = row + "" + ','

                                    } else {
                                        data = data + TagValue[i] + ','
                                        row = row + TagValue[i] + ','
                                    }
                                }
                                if (TagValue[106] != undefined && i == 132) {
                                    flag106T116 = true
                                    latest106T116Row = row.slice(0, -1) + '\n'
                                }
                                if (TagValue[17] != undefined && i == 132) {
                                    flag17T30 = true
                                }
                                if (TagValue[1] != undefined && i == 132) {
                                    flagTag1 = true
                                }
                                if (i == 1) {
                                    if (TagValue[i] != undefined) {
                                        startTime = new Date(result3.recordset[0].TagTimestamp.toISOString())
                                        endTime = new Date(result3.recordset[j].TagTimestamp.toISOString())
                                        TagRepeatCount++
                                    }
                                }
                            }
                            data = data.slice(0, -1) + '\n'
                        }
 if (flag106T116 !== true) {
                            var dataRows = data.split("\n")
                            var newdata = ""
                            var dataRowValues = dataRows[0].split(",")
                            var newdataRow = ""
                            for (var p = 0; p < dataRowValues.length; p++) {
                                if (p >= 106 && p <= 116) {

                                    var rowDataLatestArray = latest106T116Row.split(",")
                                    if (rowDataLatestArray[p] == "") {
                                        newdataRow = newdataRow + "NaN" + ","
                                    } else {
                                        newdataRow = newdataRow + rowDataLatestArray[p] + ","
                                    }
                                } else if (p != dataRowValues.length - 1) {
                                    newdataRow = newdataRow + dataRowValues[p] + ","
                                } else {
                                    newdataRow = newdataRow + dataRowValues[p]
                                }
                            }

                            dataRows[0] = newdataRow;
                            data = dataRows.join('\n');
                        }

                        var DirPath = FileDataPath + "/" +  moment(FileTimestamp,'YYYYMMDDHHmmss').format('YYYYMMDD') + "/"
                        if (flagTag1 == true) {
                            if (flag17T30 == true) {
                                interval = Math.floor((endTime.getTime() - startTime.getTime()) / 1000)
                                if (Math.floor((endTime.getTime() - startTime.getTime()) / 1000) <= 60) {
                                    var DataPath = DirPath + FileTimestamp + "_" + serialNumber + "_" + interval + "_" + rowCount + "_" + partId + ".csv"
                                    fse.mkdirp(DirPath, error => {
                                        if (error) {
                                            console.log(error)
                                        } else {
                                            fs.open(DataPath, 'a', (err, fd) => {
                                                if (err) {
                                                    console.log(err)
                                                } else {
                                                    fs.appendFile(fd, data, 'utf8', (err) => {
                                                        fs.close(fd, (err) => { if (err) console.log(err) });
                                                        if (err) console.log(err);
                                                        SerialNumberLUTcompleted.insertOne(
                                                            {
                                                                SerialNumberId: result1.recordset[i].SerialNumberId,
                                                                minTagTimestamp: result3.recordset[0].TagTimestamp,
                                                                maxTagTimestamp: result3.recordset[result3.recordset.length - 1].TagTimestamp
                                                            }, err => { })
                                                    });
                                                }
                                            });
                                        }
                                    });
                                } else {
                                    SerialNumberLUTdiscarded.insertOne(
                                        {
                                            SerialNumberId: result1.recordset[i].SerialNumberId,
 reason: "Not in between 0 to 60 interval",
                                            minTagTimestamp: result3.recordset[0] != undefined ? result3.recordset[0].TagTimestamp : "notfound",
                                            maxTagTimestamp: result3.recordset[result3.recordset.length - 1] != undefined ? result3.recordset[result3.recordset.length - 1].TagTimestamp : "notfound"
                                        }, err => { })
                                }
                            } else {
                                SerialNumberLUTdiscarded.insertOne(
                                    {
                                        SerialNumberId: result1.recordset[i].SerialNumberId,
                                        reason: "No TagValues present from 17 to 30",
                                        minTagTimestamp: result3.recordset[0] != undefined ? result3.recordset[0].TagTimestamp : "notfound",
                                        maxTagTimestamp: result3.recordset[result3.recordset.length - 1] != undefined ? result3.recordset[result3.recordset.length - 1].TagTimestamp : "notfound"
                                    }, err => { })

                            }
                        } else {
                            SerialNumberLUTdiscarded.insertOne(
                                {
                                    SerialNumberId: result1.recordset[i].SerialNumberId,
                                    reason: "value  for Tag one is not present",
                                    minTagTimestamp: result3.recordset[0] != undefined ? result3.recordset[0].TagTimestamp : "notfound",
                                    maxTagTimestamp: result3.recordset[result3.recordset.length - 1] != undefined ? result3.recordset[result3.recordset.length - 1].TagTimestamp : "notfound"
                                }, err => { })
                        }
                    } else {
                        SerialNumberLUTincomplete.insertOne(
                            {
                                SerialNumberId: result1.recordset[i].SerialNumberId,
                                status: "Incomplete",
                                minTagTimestamp: result3.recordset[0].TagTimestamp,
                            }, err => { })
                    }
                    }
                }
//code starts for checking incomplete serial numbers
                var checkdate = new Date()
                checkdate.setMinutes(checkdate.getMinutes() - 15)
                SerialNumberLUTincomplete.find({ minTagTimestamp: { $gt: checkdate } }).toArray(async (err, completedResult) => {
                    if (completedResult.length != 0) {
                        for (var q = 0; q < completedResult.length; q++) {
                            //Removing the completed serial number from SerialNumberLUTincomplete collection
                            SerialNumberLUTincomplete.remove({
                                SerialNumberId: completedResult[q].SerialNumberId
                            }, async (err, SerialNumberIdResult) => {
                                if (err) {
                                    console.log(err)
                                } else {
                                    var result2 = await request.query('select PartId from SerialNumber where SerialNumberId=' + completedResult[q].SerialNumberId)
                                    var result3 = await request.query("select distinct(TagTimestamp) from TagHistory where SerialNumberId=" + completedResult[q].SerialNumberId + " order by TagTimestamp ASC")
                                    var serialNumberStartTime = result3.recordset[0].TagTimestamp;
                                    TagRepeatCount = 0;
                                    var data = ""
                                    var partId, serialNumber, interval, rowCount
                                    partId = result2.recordset[0] != undefined ? result2.recordset[0].PartId : "default"
                                    serialNumber = result1.recordset[i].SerialNumberId
                                    rowCount = result3.recordset.length
                                    var startTime = new Date(), endTime = new Date()
                                    flag106T116 = false
                                    flag17T30 = false
                                    flagTag1 = false


                                    if (Math.floor(((new Date()).getTime() - serialNumberStartTime.getTime()) / 1000) >= 900) {
                                        //Removing the completed serial number from SerialNumberLUTincomplete collection
                                        console.log(serialNumber)
                                        SerialNumberLUTincomplete.remove({
                                            SerialNumberId: serialNumber
                                        }, function (err, result) {
                                            if (err) {
                                                console.log(err)
                                            }
                                        })
                                    }

                                    //Loop for individual Timestamp
                                    if (result3.recordset.length != 0) { FileTimestamp = moment(new Date(result3.recordset[0].TagTimestamp.toISOString())).format('YYYYMMDDHHmmss') }
                                    for (let j = 0; j < result3.recordset.length; j++) {
                                        Timestamp = moment(new Date(result3.recordset[j].TagTimestamp.toISOString())).format('YYYYMMDD')
                                        var result4 = await request.query(`SELECT a.TagId,a.TagValue FROM TagHistory a INNER JOIN (select MIN(TagHistoryId) as TagHistoryId,TagId from TagHistory where SerialNumberId=` + result1.recordset[
i].SerialNumberId + ` and TagTimestamp= '` + result3.recordset[j].TagTimestamp.toISOString() + `' group by TagId) AS b ON a.TagHistoryId = b.TagHistoryId AND a.TagId = b.TagId  where a.TagTimestamp > ` + ts1 + ` order by TagId ASC`)
                                        var TagValue = []
                                        var row = ""
                                        result4.recordset.forEach((record, index) => {
                                            TagValue[record["TagId"]] = record["TagValue"]
                                        }
                                        )

                                        for (let i = 0; i < 133; i++) {
                                            if (i == 0) {
                                                data = data + Math.floor(new Date(result3.recordset[j].TagTimestamp.toISOString()).getTime() - new Date(result3.recordset[0].TagTimestamp.toISOString()).getTime()) + ','
                                                row = row + ','
                                            }
                                            else {
                                                if (TagValue[i] == undefined) {
                                                    data = data + "" + ','
                                                     row = row + "" + ','

                                                } else {
                                                    data = data + TagValue[i] + ','
                                                    row = row + TagValue[i] + ','
                                                }
                                            }
                                            if (TagValue[106] != undefined && i == 132) {
                                                flag106T116 = true
                                                latest106T116Row = row.slice(0, -1) + '\n'
                                            }
                                            if (TagValue[17] != undefined && i == 132) {
                                                flag17T30 = true
                                            }
                                            if (TagValue[1] != undefined && i == 132) {
                                                flagTag1 = true
                                            }
                                            if (i == 1) {
                                                if (TagValue[i] != undefined) {
                                                    startTime = new Date(result3.recordset[0].TagTimestamp.toISOString())
                                                    endTime = new Date(result3.recordset[j].TagTimestamp.toISOString())

                                                }
                                            }
                                        }
                                        data = data.slice(0, -1) + '\n'
                                    }

                                    if (flag106T116 !== true) {
                                        var dataRows = data.split("\n")
                                        var newdata = ""
                                        var dataRowValues = dataRows[0].split(",")
                                        var newdataRow = ""
                                        for (var p = 0; p < dataRowValues.length; p++) {
                                            if (p >= 106 && p <= 116) {

                                                var rowDataLatestArray = latest106T116Row.split(",")
                                                if (rowDataLatestArray[p] == "") {
                                                    newdataRow = newdataRow + "NaN" + ","
                                                } else {
                                                    newdataRow = newdataRow + rowDataLatestArray[p] + ","
                                                }
                                            } else if (p != dataRowValues.length - 1) {
                                                newdataRow = newdataRow + dataRowValues[p] + ","
                                            } else {
                                                newdataRow = newdataRow + dataRowValues[p]
                                            }
                                        }

                                        dataRows[0] = newdataRow;
                                        data = dataRows.join('\n');
                                    }
                                    var DirPath = FileDataPath + "/" + moment(FileTimestamp,'YYYYMMDDHHmmss').format('YYYYMMDD') + "/"
                                    if (flagTag1 == true) {
                                        if (flag17T30 == true) {
                                            interval = Math.floor((endTime.getTime() - startTime.getTime()) / 1000)
                                            if (Math.floor((endTime.getTime() - startTime.getTime()) / 1000) <= 60) {
                                                var DataPath = DirPath + FileTimestamp + "_" + serialNumber + "_" + interval + "_" + rowCount + "_" + partId + ".csv"
                                                fse.mkdirp(DirPath, error => {
                                                    if (error) {
                                                        console.log(error)
                                                    } else {
                                                        fs.open(DataPath, 'a', (err, fd) => {
                                                            if (err) {
                                                                console.log(err)
                                                            } else {
                                                                fs.appendFile(fd, data, 'utf8', (err) => {
                                                                    fs.close(fd, (err) => { if (err) console.log(err) });
                                                                    if (err) console.log(err);
                                                                    SerialNumberLUTcompleted.insertOne(
                                                                        {
                                                                            SerialNumberId: result1.recordset[i].SerialNumberId,
                                                                            minTagTimestamp: result3.recordset[0].TagTimestamp,
                                                                            maxTagTimestamp: result3.recordset[result3.recordset.length - 1].TagTimestamp
                                                                        }, err => { })
                                                                });
                                                            }
                                                        });
                                                    }
                                                });
                                            } else {
                                                SerialNumberLUTdiscarded.insertOne(
                                                    {
                                                        SerialNumberId: result1.recordset[i].SerialNumberId,
                                                        reason: "Not in between 0 to 60 interval",
                                                        minTagTimestamp: result3.recordset[0] != undefined ? result3.recordset[0].TagTimestamp : "notfound",
                                                        maxTagTimestamp: result3.recordset[result3.recordset.length - 1] != undefined ? result3.recordset[result3.recordset.length - 1].TagTimestamp : "notfound"
                                                    }, err => { })
                                            }
                                        } else {
                                            SerialNumberLUTdiscarded.insertOne(
                                                {
                                                    SerialNumberId: result1.recordset[i].SerialNumberId,
                                                    reason: "No TagValues present from 17 to 30",
                                                    minTagTimestamp: result3.recordset[0] != undefined ? result3.recordset[0].TagTimestamp : "notfound",
                                                    maxTagTimestamp: result3.recordset[result3.recordset.length - 1] != undefined ? result3.recordset[result3.recordset.length - 1].TagTimestamp : "notfound"
                                                }, err => { })

                                        }
                                    } else {
                                        SerialNumberLUTdiscarded.insertOne(
                                            {
                                                SerialNumberId: result1.recordset[i].SerialNumberId,
                                                reason: "value  for Tag one is not present",
                                                minTagTimestamp: result3.recordset[0] != undefined ? result3.recordset[0].TagTimestamp : "notfound",
                                                maxTagTimestamp: result3.recordset[result3.recordset.length - 1] != undefined ? result3.recordset[result3.recordset.length - 1].TagTimestamp : "notfound"
                                            }, err => { })
                                    }

                                }
                            })

                        }
                         }
                })
                //end of checking incomplete serial numbers
                await collection.update({
                    "id": 95
                }, {
                        "$set": {
                            lastRunTime: newlastRunTime, latest106T116Row: latest106T116Row
                        }
                    },
                    function (err) {
                        if (err) {
                            console.log(err);
                        } else {
                            console.log("success")
                        }
                    })

                pool.close()
                db.close()
            } catch (e) {
                console.log(e)
                pool.close()
                db.close()
            }
        })

    }
    MSSQLtoCSV('/home/matlab/whirlpoolCSV')
})
                                            



