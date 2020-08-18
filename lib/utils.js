function isInverse(predicate) {
  return predicate && predicate.startsWith('^');
}

function sparqlEscapePredicate(predicate) {
  return isInverse(predicate) ? `^<${predicate.slice(1)}>` : `<${predicate}>`;
}

export {
  isInverse,
  sparqlEscapePredicate
};
