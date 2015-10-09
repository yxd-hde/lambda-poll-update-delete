module.exports = Delete;

function Delete() {
}

Delete.ids = {};

Delete.append = function(domain, id) {
  var ids = Delete.ids;
  if (!(domain in ids)) {
    ids[domain] = [];
  }

  ids[domain].push(id);
};
