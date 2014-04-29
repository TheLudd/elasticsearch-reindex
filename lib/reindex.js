var es = require('elasticsearch');
var log4js = require('log4js');
var logger = log4js.getLogger('es-reindex');

var matchAll = {
    query: {
        match_all: {}
    }
};

module.exports = function(f, t) {

    var logStart = function(number) {
        var s = 'Reindexing %s documents from %s/%s to %s/%s';
        logger.info(s, number, f.host, f.index, t.host, t.index);
    };

    var from = new es.Client({
        host: f.host
    });

    var to = new es.Client({
        host: t.host
    });

    var createBulk = function (docList) {
        return docList.reduce(function(sum, item) {
            var op = {
                create: {
                    _index: t.index,
                    _type: item._type,
                    _id: item._id
                }
            };

            if (item.fields && item.fields._parent) {
                op.create._parent = item.fields._parent;
            }

            var source = item._source;
            return sum.concat(op, source);
        }, []);
    };

    var hasMore = function(scrollDoc) {
        return scrollDoc.hits.hits.length !== 0;
    };

    var scrollNext = function(scrollDoc){
        return from.scroll({
            scrollId: scrollDoc._scroll_id,
            scroll: '2m',
            body: matchAll
        });
    };

    var step = function(scrollDoc) {
        return handleScroll(scrollDoc).then(scrollNext);
    };

    var handleScroll = function(doc) {
        var bulk = createBulk(doc.hits.hits);
        return to.bulk({
            body: bulk
        })
            .then(function(r) {
                return doc;
            });
    };

    var process = function(initial) {
        return step(initial)
            .then(function(current) {
                if (hasMore(current)) {
                    return process(current);
                }
            });
    };

    return from.count({
        index: f.index
    })
        .then(function(countDoc) {
            var count = countDoc.count;
            return from.search({
                index: f.index,
                fields:['_source', '_parent'],
                searchType: 'scan',
                scroll: '2m',
                size: 200,
                body: matchAll
            })
        })
        .then(function(doc) {
            logStart(doc.hits.total);
            return doc;
        })
        .then(scrollNext)
        .then(process)
        .error(console.log)
        .finally(from.close.bind(from))
        .finally(to.close.bind(to))
        .finally(function() {
            logger.info('Reindexing complete');
        });

};

