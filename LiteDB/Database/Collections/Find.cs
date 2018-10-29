using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace LiteDB
{
    public partial class LiteCollection<T>
    {
        #region Find

        /// <summary>
        /// Find documents inside a collection using Query object.
        /// </summary>
        public IEnumerable<T> Find(Query query, int skip = 0, int limit = int.MaxValue, IDictionary<Type, Type> typeReplacement = null)
        {
            if (query == null) throw new ArgumentNullException(nameof(query));

            var docs = _engine.Value.Find(_name, query, _includes.ToArray(), skip, limit);

            foreach(var doc in docs)
            {
                // get object from BsonDocument
                var obj = _mapper.ToObject<T>(doc, typeReplacement);

                yield return obj;
            }
        }

        /// <summary>
        /// Find documents inside a collection using Linq expression. Must have indexes in linq expression
        /// </summary>
        public IEnumerable<T> Find(Expression<Func<T, bool>> predicate, int skip = 0, int limit = int.MaxValue, IDictionary<Type, Type> typeReplacement = null)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));

            return this.Find(_visitor.Visit(predicate), skip, limit, typeReplacement);
        }

        #endregion

        #region FindById + One + All

        /// <summary>
        /// Find a document using Document Id. Returns null if not found.
        /// </summary>
        public T FindById(BsonValue id, IDictionary<Type, Type> typeReplacement = null)
        {
            if (id == null || id.IsNull) throw new ArgumentNullException(nameof(id));

            return this.Find(Query.EQ("_id", id), typeReplacement: typeReplacement).SingleOrDefault();
        }

        /// <summary>
        /// Find the first document using Query object. Returns null if not found. Must have index on query expression.
        /// </summary>
        public T FindOne(Query query, IDictionary<Type, Type> typeReplacement = null)
        {
            return this.Find(query, typeReplacement: typeReplacement).FirstOrDefault();
        }

        /// <summary>
        /// Find the first document using Linq expression. Returns null if not found. Must have indexes on predicate.
        /// </summary>
        public T FindOne(Expression<Func<T, bool>> predicate, IDictionary<Type, Type> typeReplacement = null)
        {
            return this.Find(predicate, typeReplacement: typeReplacement).FirstOrDefault();
        }

        /// <summary>
        /// Returns all documents inside collection order by _id index.
        /// </summary>
        public IEnumerable<T> FindAll(IDictionary<Type, Type> typeReplacement = null)
        {
            return this.Find(Query.All(), typeReplacement: typeReplacement);
        }

        #endregion
    }
}