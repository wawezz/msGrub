const sql = require('mssql')
const mysql = require('mysql')
const moment = require('moment')
const mongodb = require('mongodb').MongoClient
const uuidv1 = require('uuid/v1')

const userId = '0835c4a7-497c-11e9-bf47-0242ac190006'
const mssqlConf = {
    user: 'SA',
    password: 'Asdf!234',
    db: 'ratetel',
    host: 'localhost'
}
const mongoConf = {
    user: 'admin',
    password: 'zmVcMLV3E2XhyYZDb3EqEyb44XPFB2Qr',
    db: 'map-crm',
    collection: 'notes',
    host: 'localhost',
    port: 44017
}
const mysqlConf = {
    port: 43266,
    host: 'localhost',
    user: 'map-crm',
    password: '6529e36bc27f81f4c527f6d31bf1',
    database: 'map-crm',
    charset: 'utf8mb4_unicode_ci'
}

const sleep = (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
};

const con = async () => {
    try {
        await sql.connect('mssql://' + mssqlConf.user + ':' + mssqlConf.password + '@' + mssqlConf.host + '/' + mssqlConf.db)

        const result = await sql.query`select * from places left join placeInfo on places.placeID = placeInfo.placeId left join placeDetails on places.placeID = placeDetails.placeID`

        const connection = mysql.createConnection(mysqlConf);

        connection.connect();

        const records = result.recordset;

        let IDs = [];
        let query = [];
        let updateQuery = [];
        let defaultSql = 'INSERT INTO app_place_leads (id, geo) VALUES ';
        for (let i = 0; i < records.length; i++) {

            let points = null;
            let record = records[i];
            if (record.geo) {
                const point = record.geo.points[0];

                points = "ST_GeomFromText('POINT(" + point.x + " " + point.y + ")') ";
            }

            IDs.push(record.placeID[0]);

            let sqlR = '';
            sqlR += '( ';
            sqlR += `'${record.placeID[0]}', `;
            sqlR += `${points}`;
            sqlR += ')';

            updateQuery.push(sqlR);

            query.push([
                record.placeID[0],
                record.name,
                record.address,
                record.telephone,
                record.type,
                record.status,
                record.price,
                record.rating,
                record.review,
                record.website,
                null,
                record.data,
                record.ToSync,
                record.CampaignCode,
                record.IsImportant,
                userId,
                moment(record.InsertDate).format("YYYY-MM-DD HH:mm:ss"),
                moment(record.lastUpdate).format("YYYY-MM-DD HH:mm:ss"),
                parseInt(moment(record.contractDate).format("YYYY")) >= 1970 ? moment(record.contractDate).format("YYYY-MM-DD HH:mm:ss") : null,
                parseInt(moment(record.NextFollowupDate).format("YYYY")) >= 1970 ? moment(record.NextFollowupDate).format("YYYY-MM-DD HH:mm:ss") : null
            ]);

            if ((i + 1) % 1000 === 0) {
                console.log(i);

                connection.query('INSERT INTO app_place_leads ( id, name, address, phone, type, status, price, rating, review, website, geo, data, toSync, campaignCode, isImportant, createdBy, createdAt, updatedAt, contractAt, nextFollowupDate) VALUES ?', [query], function (err) {
                    if (err) throw err;
                });

                query = [];
                await sleep(100);

                const insertSql = defaultSql + updateQuery.join(', ') + ' ON DUPLICATE KEY UPDATE geo = VALUES(geo)';
                connection.query(insertSql, function (err) {
                    if (err) throw err;
                });

                updateQuery = [];

                await sleep(100);
            }
        }

        if (query.length) {
            connection.query('INSERT INTO app_place_leads ( id, name, address, phone, type, status, price, rating, review, website, geo, data, toSync, campaignCode, isImportant, createdBy, createdAt, updatedAt, contractAt, nextFollowupDate) VALUES ?', [query], function (err) {
                if (err) throw err;
            });

            query = [];
        }

        if (updateQuery.length) {
            const insertSql = defaultSql + updateQuery.join(', ') + ' ON DUPLICATE KEY UPDATE geo = VALUES(geo)';
            connection.query(insertSql, function (err) {
                if (err) throw err;
            });

            updateQuery = [];
        }

        await sleep(100);

        const resultNotes = await sql.query`select * from placesNotelog`

        const url = 'mongodb://' + mongoConf.host + ':' + mongoConf.port + '/';

        const notes = resultNotes.recordset;
        let notesObj = [];

        for (let n = 0; n < notes.length; n++) {
            let note = notes[n];

            if (IDs.includes(note.PlaceID)) {
                notesObj.push({
                    _id: uuidv1(),
                    elementId: note.PlaceID,
                    elementType: 16,
                    noteType: 6,
                    createdBy: userId,
                    createdAt: moment(note.date).unix(),
                    updatedAt: moment(note.date).unix(),
                    dataValue: { workerId: note.update_worker_id }
                });
            }
        }

        mongodb.connect(url, {
            useNewUrlParser: true,
            auth: {
                user: mongoConf.user,
                password: mongoConf.password,
            }
        }, function (err, db) {
            if (err) throw err;
            const dbo = db.db(mongoConf.db);
            dbo.collection(mongoConf.collection).insertMany(notesObj, function (err, res) {
                if (err) throw err;
                console.log("Number of documents inserted: " + res.insertedCount);
                db.close();
            });
        });

        console.log('end');

        connection.end();

    } catch (err) {
        console.log(err);
    }
}

con();