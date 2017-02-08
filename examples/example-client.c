/* gcc example-client.c -o example-client $(pkg-config --cflags --libs
 * libmongoc-1.0) */

/* ./example-client [CONNECTION_STRING [COLLECTION_NAME]] */

#include <mongoc.h>
#include <stdio.h>
#include <stdlib.h>

#define MONGOC_COMPILATION
#include <mongoc-client-private.h>

#define MONGOC_STREAM_INPROC 8

static mongoc_stream_t *default_stream;


void *
mongo_TransportLayerInProcConnectionConnect (const char *addr);

ssize_t
mongo_TransportLayerInProcConnectionSend (void *connection,
                                          const void *buffer,
                                          size_t bufferLen,
                                          int32_t timeout);
ssize_t
mongo_TransportLayerInProcConnectionRecv (void *connection,
                                          void *buffer,
                                          size_t bufferLen,
                                          int32_t timeout);

void
mongo_TransportLayerInProcConnectionClose (void *connection);


typedef struct _mongoc_stream_inproc_data_t {
   mongoc_client_t *client;
} mongoc_stream_inproc_data_t;


typedef struct _mongoc_stream_inproc_t {
   mongoc_stream_t vtable;
   mongoc_stream_t *wrapped;
   mongoc_stream_inproc_data_t *data;
} mongoc_stream_inproc_t;


static int
_mongoc_stream_inproc_close (mongoc_stream_t *stream)
{
   return mongoc_stream_close (((mongoc_stream_inproc_t *) stream)->wrapped);
}


static void
_mongoc_stream_inproc_destroy (mongoc_stream_t *stream)
{
   mongoc_stream_inproc_t *debug_stream = (mongoc_stream_inproc_t *) stream;

   mongoc_stream_destroy (debug_stream->wrapped);
   bson_free (debug_stream);
}


static void
_mongoc_stream_inproc_failed (mongoc_stream_t *stream)
{
   mongoc_stream_inproc_t *debug_stream = (mongoc_stream_inproc_t *) stream;

   mongoc_stream_failed (debug_stream->wrapped);
   bson_free (debug_stream);
}


static int
_mongoc_stream_inproc_setsockopt (mongoc_stream_t *stream,
                                  int level,
                                  int optname,
                                  void *optval,
                                  mongoc_socklen_t optlen)
{
   return mongoc_stream_setsockopt (
      ((mongoc_stream_inproc_t *) stream)->wrapped,
      level,
      optname,
      optval,
      optlen);
}


static int
_mongoc_stream_inproc_flush (mongoc_stream_t *stream)
{
   return mongoc_stream_flush (((mongoc_stream_inproc_t *) stream)->wrapped);
}


static ssize_t
_mongoc_stream_inproc_readv (mongoc_stream_t *stream,
                             mongoc_iovec_t *iov,
                             size_t iovcnt,
                             size_t min_bytes,
                             int32_t timeout_msec)
{
   return mongoc_stream_readv (((mongoc_stream_inproc_t *) stream)->wrapped,
                               iov,
                               iovcnt,
                               min_bytes,
                               timeout_msec);
}


static ssize_t
_mongoc_stream_inproc_writev (mongoc_stream_t *stream,
                              mongoc_iovec_t *iov,
                              size_t iovcnt,
                              int32_t timeout_msec)
{
   return mongoc_stream_writev (
      ((mongoc_stream_inproc_t *) stream)->wrapped, iov, iovcnt, timeout_msec);
}


static bool
_mongoc_stream_inproc_check_closed (mongoc_stream_t *stream)
{
   return mongoc_stream_check_closed (
      ((mongoc_stream_inproc_t *) stream)->wrapped);
}


static mongoc_stream_t *
_mongoc_stream_inproc_get_base_stream (mongoc_stream_t *stream)
{
   mongoc_stream_t *wrapped = ((mongoc_stream_inproc_t *) stream)->wrapped;

   /* "wrapped" is typically a mongoc_stream_buffered_t, get the real
    * base stream */
   if (wrapped->get_base_stream) {
      return wrapped->get_base_stream (wrapped);
   }

   return wrapped;
}


mongoc_stream_t *
inproc_stream_new (mongoc_stream_inproc_data_t *data)
{
   mongoc_stream_inproc_t *s;

   assert (default_stream);

   s = (mongoc_stream_inproc_t *) bson_malloc0 (sizeof *s);

   s->vtable.type = MONGOC_STREAM_INPROC;
   s->vtable.close = _mongoc_stream_inproc_close;
   s->vtable.destroy = _mongoc_stream_inproc_destroy;
   s->vtable.failed = _mongoc_stream_inproc_failed;
   s->vtable.flush = _mongoc_stream_inproc_flush;
   s->vtable.readv = _mongoc_stream_inproc_readv;
   s->vtable.writev = _mongoc_stream_inproc_writev;
   s->vtable.setsockopt = _mongoc_stream_inproc_setsockopt;
   s->vtable.check_closed = _mongoc_stream_inproc_check_closed;
   s->vtable.get_base_stream = _mongoc_stream_inproc_get_base_stream;

   s->wrapped = default_stream;
   s->data = data;

   return (mongoc_stream_t *) s;
}


mongoc_stream_t *
inproc_stream_initiator (const mongoc_uri_t *uri,
                         const mongoc_host_list_t *host,
                         void *user_data,
                         bson_error_t *error)
{
   mongoc_stream_inproc_data_t *data;

   data = (mongoc_stream_inproc_data_t *) user_data;

   return inproc_stream_new (data);
}


int
main (int argc, char *argv[])
{
   mongoc_client_t *client;
   mongoc_stream_inproc_data_t data;
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
      fprintf (stderr, "Failed to parse URI\n");
      return EXIT_FAILURE;
   }

   data.client = client = mongoc_client_new_from_uri (uri);
   default_stream = mongoc_client_default_stream_initiator (
      uri, mongoc_uri_get_hosts (uri), client, &error);

   if (!default_stream) {
      fprintf (stderr, "Couldn't create stream: %s\n", error.message);
      return EXIT_FAILURE;
   }

   mongoc_client_set_stream_initiator (client, inproc_stream_initiator, &data);

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
      fprintf (stderr, "Cursor Failure: %s\n", error.message);
      return EXIT_FAILURE;
   }

   bson_destroy (&query);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);

   mongoc_cleanup ();

   return EXIT_SUCCESS;
}
