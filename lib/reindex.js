var es = require('elasticsearch');
var matchAll = {
    query: {
        match_all: {}
    }
};
var count = 0;

module.exports = function(f, t) {
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
            var source = item._source;
            return sum.concat(op, source);
        }, []);
    };

    var scrollNext = function(scrollDoc){
        return from.scroll({
            scrollId: scrollDoc._scroll_id,
            scroll: '2m',
            body: matchAll
        });
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

    var step = function(scrollDoc) {
        console.log('Stepping', scrollDoc.hits.hits.length);
        count += scrollDoc.hits.hits.length;
        return handleScroll(scrollDoc).then(scrollNext);
    };

    var hasMore = function(scrollDoc) {
        return scrollDoc.hits.hits.length !== 0;
    };

    var process = function(initial) {
        return step(initial)
            .then(function(current) {
                if (hasMore(current)) {
                    return process(current);
                }
            });
    };

    from.count({
        index: f.index
    })
        .then(function(countDoc) {
            var count = countDoc.count;
            console.log(count);
            return from.search({
                index: f.index,
                searchType: 'scan',
                scroll: '2m',
                size: 200,
                body: matchAll
            })
        })
        .then(function(doc) {
            console.log("Reindexing %s documents", doc.hits.total);
            return doc;
        })
        .then(scrollNext)
        .then(process)
        .error(console.log)
        .finally(from.close.bind(from))
        .finally(to.close.bind(to))

};

