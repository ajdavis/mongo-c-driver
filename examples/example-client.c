/* gcc example-client.c -o example-client $(pkg-config --cflags --libs
 * libmongoc-1.0) */

/* ./example-client [CONNECTION_STRING [COLLECTION_NAME]] */

#include <mongoc.h>

/* for access to mongoc_client_default_stream_initiator */
#define MONGOC_COMPILATION
#include <mongoc-client-private.h>

#define MONGOC_STREAM_INPROC 8

static mongoc_stream_t *default_stream;


void *
mongo_TransportLayerInProcConnectionConnect (const char *addr)
{
   assert (default_stream);

   return default_stream;
}


ssize_t
mongo_TransportLayerInProcConnectionPoll (mongoc_stream_poll_t *streams,
                                          size_t nstreams)
{
   return mongoc_stream_poll (streams, nstreams, -1);
}


ssize_t
mongo_TransportLayerInProcConnectionWriteV (void *connection,
                                            mongoc_iovec_t *iov,
                                            size_t iovcnt)
{
   return mongoc_stream_writev (
      (mongoc_stream_t *) connection, iov, iovcnt, -1);
}


ssize_t
mongo_TransportLayerInProcConnectionReadV (void *connection,
                                           mongoc_iovec_t *iov,
                                           size_t iovcnt)
{
   size_t i;
   size_t buf_len;

   buf_len = 0;
   for (i = 0; i < iovcnt; i++) {
      buf_len += iov[i].iov_len;
   }

   return mongoc_stream_readv (
      (mongoc_stream_t *) connection, iov, iovcnt, buf_len /* min bytes */, -1);
}


void
mongo_TransportLayerInProcConnectionClose (void *connection)
{
   if (mongoc_stream_close ((mongoc_stream_t *) connection) < 0) {
      MONGOC_ERROR ("closing default stream\n");
   }
}


typedef struct _mongoc_stream_inproc_t {
   mongoc_stream_t vtable;
   mongoc_stream_t *wrapped;
} mongoc_stream_inproc_t;


static int
_mongoc_stream_inproc_close (mongoc_stream_t *stream)
{
   mongo_TransportLayerInProcConnectionClose (
      ((mongoc_stream_inproc_t *) stream)->wrapped);

   return 0;
}


static void
_mongoc_stream_inproc_destroy (mongoc_stream_t *stream)
{
   /* nothing */
}


static ssize_t
_mongoc_stream_inproc_poll (mongoc_stream_poll_t *streams,
                            size_t nstreams,
                            int32_t timeout)
{
   mongoc_stream_poll_t default_poller;
   ssize_t ret;

   default_poller.stream = (((mongoc_stream_inproc_t *) streams->stream)->wrapped);
   default_poller.events = streams->events;
   default_poller.revents = 0;

   ret = mongo_TransportLayerInProcConnectionPoll (&default_poller, 1);
   streams->revents = default_poller.revents;

   return ret;
}


static ssize_t
_mongoc_stream_inproc_readv (mongoc_stream_t *stream,
                             mongoc_iovec_t *iov,
                             size_t iovcnt,
                             size_t min_bytes,
                             int32_t timeout_msec)
{
   return mongo_TransportLayerInProcConnectionReadV (
      ((mongoc_stream_inproc_t *) stream)->wrapped, iov, iovcnt);
}


static ssize_t
_mongoc_stream_inproc_writev (mongoc_stream_t *stream,
                              mongoc_iovec_t *iov,
                              size_t iovcnt,
                              int32_t timeout_msec)
{
   return mongo_TransportLayerInProcConnectionWriteV (
      ((mongoc_stream_inproc_t *) stream)->wrapped, iov, iovcnt);
}


mongoc_stream_t *
inproc_stream_initiator (const mongoc_uri_t *uri,
                         const mongoc_host_list_t *host,
                         void *user_data,
                         bson_error_t *error)
{
   mongoc_stream_t *wrapped;
   mongoc_stream_inproc_t *s;

   wrapped = mongo_TransportLayerInProcConnectionConnect (host->host_and_port);
   if (!wrapped) {
      return NULL;
   }

   s = (mongoc_stream_inproc_t *) bson_malloc0 (sizeof *s);

   s->vtable.type = MONGOC_STREAM_INPROC;
   s->vtable.poll = _mongoc_stream_inproc_poll;
   s->vtable.close = _mongoc_stream_inproc_close;
   s->vtable.readv = _mongoc_stream_inproc_readv;
   s->vtable.writev = _mongoc_stream_inproc_writev;

   /* driver requires the destructor to be non-null */
   s->vtable.destroy = _mongoc_stream_inproc_destroy;

   s->wrapped = wrapped;

   return (mongoc_stream_t *) s;
}


int
main (int argc, char *argv[])
{
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   bson_error_t error;
   const bson_t *doc;
   const char *uristr = "mongodb://127.0.0.1/";
   mongoc_uri_t *uri;
   bson_t query;
   char *str;

   mongoc_init ();

   if (argc > 1) {
      uristr = argv[1];
   }

   uri = mongoc_uri_new (uristr);
   if (!uri) {
      MONGOC_ERROR ("Failed to parse URI\n");
      return EXIT_FAILURE;
   }

   client = mongoc_client_new_from_uri (uri);
   default_stream = mongoc_client_default_stream_initiator (
      uri, mongoc_uri_get_hosts (uri), client, &error);

   if (!default_stream) {
      MONGOC_ERROR ("Couldn't create stream: %s\n", error.message);
      return EXIT_FAILURE;
   }

   mongoc_client_set_stream_initiator (client, inproc_stream_initiator, NULL);

   bson_init (&query);
   collection = mongoc_client_get_collection (client, "test", "test");
   cursor = mongoc_collection_find_with_opts (
      collection,
      &query,
      NULL,  /* additional options */
      NULL); /* read prefs, NULL for default */

   while (mongoc_cursor_next (cursor, &doc)) {
      str = bson_as_json (doc, NULL);
      fprintf (stdout, "%s\n", str);
      bson_free (str);
   }

   if (mongoc_cursor_error (cursor, &error)) {
      MONGOC_ERROR ("Cursor Failure: %s\n", error.message);
      return EXIT_FAILURE;
   }

   bson_destroy (&query);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mongoc_stream_destroy (default_stream);
   mongoc_uri_destroy (uri);

   mongoc_cleanup ();

   return EXIT_SUCCESS;
}
