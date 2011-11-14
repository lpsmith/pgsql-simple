{-# OPTIONS_GHC -Wall -fno-warn-name-shadowing #-}
{-# LANGUAGE RecordWildCards, OverloadedStrings, ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts, ViewPatterns, NamedFieldPuns, TupleSections #-}
{-# LANGUAGE DoAndIfThenElse #-}

-- | A front-end implementation for the PostgreSQL database protocol
--   version 3.0 (implemented in PostgreSQL 7.4 and later).

module Database.PostgreSQL.Base
  (begin
  ,rollback
  ,commit
  ,query
  ,exec
  ,escapeBS
  ,connect
  ,defaultConnectInfo
  ,close
  ,withDB
  ,withTransaction)
  where

import           Database.PostgreSQL.Base.Types

import           Control.Concurrent
import           Control.Concurrent.Edge
import           Control.Exception
import           Control.Monad
import           Control.Monad.CatchIO          (MonadCatchIO)
import qualified Control.Monad.CatchIO          as E
import           Control.Monad.Fix
import           Control.Monad.State            (MonadState,execStateT,modify)
import           Control.Monad.Trans
import           Data.Binary
import           Data.Binary.Get
import           Data.Binary.Put
import           Data.ByteString                (ByteString)
import qualified Data.ByteString                as B
import qualified Data.ByteString.Lazy           as L
import qualified Data.ByteString.Lazy.UTF8      as L (toString,fromString)
import           Data.ByteString.UTF8           (toString,fromString)
import           Data.Int
import           Data.List
import           Data.Map                       (Map)
import qualified Data.Map                       as M
import           Data.Maybe
import           Data.Monoid
import qualified Data.Vector.Mutable            as V
import           Network
import           Prelude                        hiding (catch)
import           System.IO                      hiding (hPutStr)
import           Data.Bits((.&.))

--------------------------------------------------------------------------------
-- Exported values

-- | Default information for setting up a connection.
--
-- Defaults are as follows:
--
-- * Server on @localhost@
--
-- * User @postgres@
--
-- * No password
--
-- * Database @test@
--
-- * Character set @utf8@
--
-- Use as in the following example:
--
-- > connect defaultConnectInfo { connectHost = "db.example.com" }
defaultConnectInfo :: ConnectInfo
defaultConnectInfo = ConnectInfo {
                       connectHost = "127.0.0.1"
                     , connectPort = 5432
                     , connectUser = "postgres"
                     , connectPassword = ""
                     , connectDatabase = ""
                     }

-- | Connect with the given username to the given database. Will throw
--   an exception if it cannot connect.
connect :: MonadIO m => ConnectInfo -> m Connection -- ^ The datase connection.
connect connectInfo@ConnectInfo{..} = liftIO $ withSocketsDo $ do
  event    <- newEmptyMVar
  throttle <- newMVar ()
  request  <- newEmptyMVar
  (source, sink) <- newEdge
  notifications <- newSink
  handle   <- connectTo connectHost (PortNumber $ fromIntegral connectPort)
  hSetBuffering handle NoBuffering
  _ <- forkIO (connectionRouter event  throttle sink handle)
  _ <- forkIO (requestListener  event  throttle request)
  _ <- forkIO (responseListener event  handle notifications)
  _ <- forkIO (connectionSender source handle)
  types <- newMVar M.empty
  let conn = Connection request event notifications types
  authenticate conn connectInfo
  return conn

-- | requestListener listens for incoming database queries
-- from Haskell code,  and handles them accordingly

requestListener :: MVar Event -> MVar () -> MVar Dialog -> IO a
requestListener event throttle request = loop
  where
    loop = do
      ()  <- takeMVar throttle
      req <- takeMVar request `catch` \(e :: BlockedIndefinitelyOnMVar) -> do
                                   putMVar event (Disconnect ConnectionClosed)
                                   throwIO e
      putMVar event (Request req)
      loop

-- | responseListener listens for messages coming in on the network
-- socket from the PostgreSQL server.  Normal messages are sent back
-- to the connectionRouter to be directed to the correct destination,
-- but async notices and async notifications are routed by the
-- responseListener process directly.

responseListener :: MVar Event -> Handle -> Sink Notification -> IO a
responseListener event handle notifications = loop
  where
    loop = do
      rspMsg <- getMessage_ handle `catch` \(e :: ConnectionClosed) -> do
                                                putMVar event (Disconnect e)
                                                throwIO e
      case rspMsg of
        RspMsg 'A' msg  -> writeSink notifications (getNotification msg)
        RspMsg 'N' _msg -> return () -- FIXME: do something with async notices
        RspMsg _   _msg -> putMVar event (Response rspMsg)
      loop

-- | the ConnectionSender writes data to the network

connectionSender :: Source (Source ReqMsg) -> Handle -> IO a
connectionSender sources handle = nextSource
  where
    nextSource = loop =<< readSource sources

    loop source = do
      msg <- readSource source
--      print msg
      case msg of
        Done       -> nextSource
        ReqMsg msg -> L.hPut handle msg >> loop source

maxActiveRequests :: Int
maxActiveRequests = 8

nextIdx :: Int -> Int
{--
nextIdx n | n == maxActiveRequests - 1 = 0
          | otherwise                  = n + 1
--}

nextIdx n = (n+1).&.7

writeSinkVector :: V.IOVector (MVar (Item a)) -> Int -> a -> IO ()
writeSinkVector arr idx a = do
   new_hole <- newEmptyMVar
   hole <- V.read arr idx
   V.write arr idx new_hole
   putMVar hole (Item a new_hole)

writeAndCloseSinkVector :: Exception e
                        => V.IOVector (MVar (Item a))
                        -> Int -> a -> e -> IO ()
writeAndCloseSinkVector arr idx a e = do
   mb <- newMVar (throw e)
   hole <- V.read arr idx
   V.write arr idx mb
   putMVar hole (Item a mb)

-- | The connectionRouter is by far the most complicated process
--   in the team.   It is responsible for
--
--   1.  Hooking up new requests to the connectionSender process
--
--   2.  Throttling the rate of requests, so that we don't send
--       requests faster than the database can respond.
--
--   3.  Tracking new requests so that it can determine where
--       to send the results.
--
--   4.  Sending the results to the correct location.
--
--   5.  Handling disconnects and closing the handle.
--
-- Closing the connection turns out to be slightly tricky.  In
-- order to maintain the illusion that we aren't pulling any
-- shenanigans with unsafeInterleaveIO,  we need to immediately
-- stop accepting new requests,  but hold the database connection
-- open until the server is finished sending results.
--
-- It may make sense to add a "rude" shutdown that immediately
-- closes the connection as well.  But in most cases,  a soft shutdown
-- is what you want.

connectionRouter :: MVar Event -> MVar () -> Sink (Source ReqMsg) -> Handle
                 -> IO a
connectionRouter event throttle sender handle = do
    sinks <- V.new maxActiveRequests
    let loop reqIdx maxIdx = do
            evt <- takeMVar event
            case evt of
              Request (Dialog source sink) -> do
--                  putStrLn "request"
                  writeSink sender source
                  V.write sinks maxIdx sink
                  let idx = nextIdx maxIdx
                  when (idx /= reqIdx) (putMVar throttle ())
                  loop reqIdx idx
              Response msg@(RspMsg 'Z' _block) -> do
--                  print msg
                  writeAndCloseSinkVector sinks reqIdx msg
                                          InternalException
                  _ <- tryPutMVar throttle ()
                  loop (nextIdx reqIdx) maxIdx
              Response msg -> do
--                  print msg
                  writeSinkVector sinks reqIdx msg
                  loop reqIdx maxIdx
              Disconnect ConnectionClosed -> do
                  shutdown reqIdx maxIdx
              Disconnect ConnectionLost -> do
                  halt reqIdx maxIdx
        shutdown reqIdx maxIdx = do
            _ <- tryPutMVar throttle ()
            evt <- takeMVar event
            case evt of
              Request (Dialog _source hole) -> do
                  putMVar hole (throw ConnectionClosed)
                  shutdown reqIdx maxIdx
              Response msg@(RspMsg 'Z' _block) -> do
                  writeAndCloseSinkVector sinks reqIdx msg InternalException
                  let idx = nextIdx reqIdx
                  if idx == maxIdx
                  then do
                     -- fixme:  send a termination message
                     hClose handle
                     closed ConnectionClosed
                  else do
                     shutdown idx maxIdx
              Response msg -> do
                  writeSinkVector sinks reqIdx msg
                  shutdown reqIdx maxIdx
              Disconnect ConnectionClosed -> do
                  shutdown reqIdx maxIdx
              Disconnect ConnectionLost -> do
                  halt reqIdx maxIdx
        halt reqIdx maxIdx = do
            let idxs = takeWhile (/= maxIdx) (iterate nextIdx reqIdx)
            forM_ idxs $ \idx -> do
                hole <- V.read sinks idx
                putMVar hole (throw ConnectionLost)
            closed ConnectionLost
        closed status = do
            _ <- tryPutMVar throttle ()
            evt <- takeMVar event
            case evt of
              Request (Dialog _source hole) -> putMVar hole (throw status)
              _ -> return ()
            closed status
    loop 0 0

-- | Run a an action with a connection and close the connection
--   afterwards (protects against exceptions).
withDB :: (MonadCatchIO m,MonadIO m) => ConnectInfo -> (Connection -> m a) -> m a
withDB connectInfo m = E.bracket (liftIO $ connect connectInfo) (liftIO . close) m

-- | With a transaction, do some action (protects against exceptions).
withTransaction :: (MonadCatchIO m,MonadIO m) => Connection -> m a -> m a
withTransaction conn act = do
  begin conn
  r <- act `E.onException` rollback conn
  commit conn
  return r

-- | Rollback a transaction.
rollback :: (MonadCatchIO m,MonadIO m) => Connection -> m ()
rollback conn = do
  _ <- exec conn (fromString ("ABORT;" :: String))
  return ()

-- | Commit a transaction.
commit :: (MonadCatchIO m,MonadIO m) => Connection -> m ()
commit conn = do
  _ <- exec conn (fromString ("COMMIT;" :: String))
  return ()

-- | Begin a transaction.
begin :: (MonadCatchIO m,MonadIO m) => Connection -> m ()
begin conn = do
  _ <- exec conn (fromString ("BEGIN;" :: String))
  return ()

-- | Close a connection. Can safely be called any number of times.
-- Don't call it from a large number of threads,  otherwise you can
-- DoS the connectionRouter.
close :: MonadIO m => Connection -- ^ The connection.
      -> m ()
close Connection{connectionEvent} = liftIO $ do
    putMVar connectionEvent (Disconnect ConnectionClosed)

-- | Run a simple query on a connection.
query :: (MonadCatchIO m)
      => Connection -- ^ The connection.
      -> ByteString              -- ^ The query.
      -> m ([Field],[[Maybe ByteString]])
query conn sql = do
  result <- execQuery conn sql
  case result of
    (_,Just ok) -> return ok
    _           -> return ([],[])

-- | Run a simple query on a connection.
execQuery :: (MonadCatchIO m)
      => Connection -- ^ The connection.
      -> ByteString              -- ^ The query.
      -> m (Integer,Maybe ([Field],[[Maybe ByteString]]))
execQuery conn sql = liftIO $ do
  withConnection conn $ \h -> do
    types <- readMVar $ connectionObjects conn
    Result{..} <- sendQuery types h sql
    case resultType of
      ErrorResponse -> E.throw $ QueryError (fmap L.toString resultError)
      EmptyQueryResponse -> E.throw QueryEmpty
      _             ->
        let tagCount = fromMaybe 0 resultTagRows
        in case resultDesc of
             Just fields -> return $ (tagCount,Just (fields,resultRows))
             Nothing     -> return $ (tagCount,Nothing)

-- | Exec a command.
exec :: (MonadCatchIO m)
     => Connection
     -> ByteString
     -> m Integer
exec conn sql = do
  result <- execQuery conn sql
  case result of
    (ok,_) -> return ok

-- | PostgreSQL protocol version supported by this library.
protocolVersion :: Int32
protocolVersion = 196608

-- | Escape a string for PostgreSQL.
escape :: String -> String
escape ('\\':cs) = '\\' : '\\' : escape cs
escape ('\'':cs) = '\'' : '\'' : escape cs
escape (c:cs) = c : escape cs
escape [] = []

-- | Escape a string for PostgreSQL.
escapeBS :: ByteString -> ByteString
escapeBS = fromString . escape . toString

--------------------------------------------------------------------------------
-- Authentication

-- | Run the connectInfoentication procedure.
authenticate :: Connection -> ConnectInfo -> IO ()
authenticate conn@Connection{..} connectInfo = do
  withConnection conn $ \h -> do
    sendStartUp h connectInfo
    getConnectInfoResponse h connectInfo
  withConnection conn $ \h -> do
    objects <- objectIds h
    modifyMVar_ connectionObjects (\_ -> return objects)

-- | Send the start-up message.
sendStartUp :: PGHandle -> ConnectInfo -> IO ()
sendStartUp h ConnectInfo{..} = do
  sendBlock h Nothing $ do
    int32 protocolVersion
    string (fromString "user")     ; string (fromString connectUser)
    string (fromString "database") ; string (fromString connectDatabase)
    zero

-- | Wait for and process the connectInfoentication response from the server.
getConnectInfoResponse :: PGHandle -> ConnectInfo -> IO ()
getConnectInfoResponse h conninfo = do
  (typ,block) <- getMessage h
  -- TODO: Handle connectInfo failure. Handle information messages that are
  --       sent, maybe store in the connection value for later
  --       inspection.
  case typ of
    AuthenticationOk
      | param == 0 -> waitForReady h
      | param == 3 -> sendPassClearText h conninfo
--      | param == 5 -> sendPassMd5 h conninfo salt
      | otherwise  -> E.throw $ UnsupportedAuthenticationMethod param (show block)
        where param = decode block :: Int32
              _salt = flip runGet block $ do
                        _ <- getInt32
                        getWord8

    els -> E.throw $ AuthenticationFailed (show (els,block))

-- | Send the pass as clear text and wait for connect response.
sendPassClearText :: PGHandle -> ConnectInfo -> IO ()
sendPassClearText h conninfo@ConnectInfo{..} = do
  sendMessage h PasswordMessage $
    string (fromString connectPassword)
  getConnectInfoResponse h conninfo

-- -- | Send the pass as salted MD5 and wait for connect response.
-- sendPassMd5 :: Handle -> ConnectInfo -> Word8 -> IO ()
-- sendPassMd5 h conninfo@ConnectInfo{..} salt = do
--   -- TODO: Need md5 library with salt support.
--   sendMessage h PasswordMessage $
--     string (fromString connectPassword)
--   getConnectInfoResponse h conninfo

--------------------------------------------------------------------------------
-- Initialization

objectIds :: PGHandle -> IO (Map ObjectId String)
objectIds h = do
    Result{..} <- sendQuery M.empty h q
    case resultType of
      ErrorResponse -> E.throw $ InitializationError "Couldn't get types."
      _ -> return $ M.fromList $ catMaybes $ flip map resultRows $ \row ->
             case map toString $ catMaybes row of
               [typ,readMay -> Just objId] -> Just $ (ObjectId objId,typ)
               _                           -> Nothing

  where q = fromString ("SELECT typname, oid FROM pg_type" :: String)

--------------------------------------------------------------------------------
-- Queries and commands

-- | Send a simple query.
sendQuery :: Map ObjectId String -> PGHandle -> ByteString -> IO Result
sendQuery types h sql = do
  sendMessage h Query $ string sql
  listener $ \continue -> do
    (typ,block) <- liftIO $ getMessage h
    let setStatus = modify $ \r -> r { resultType = typ }
    case typ of
      ReadyForQuery ->
        modify $ \r -> r { resultRows = reverse (resultRows r) }

      listenPassively -> do
        case listenPassively of
          EmptyQueryResponse -> setStatus
          CommandComplete    -> do setStatus
                                   setCommandTag block
          ErrorResponse -> do
            modify $ \r -> r { resultError = Just block }
            setStatus
          RowDescription -> getRowDesc types block
          DataRow        -> getDataRow block
          _ -> return ()

        continue

  where emptyResponse = Result [] Nothing Nothing [] UnknownMessageType Nothing
        listener m = execStateT (fix m) emptyResponse

-- | CommandComplete returns a ‘tag’ which indicates how many rows were
-- affected, or returned, as a result of the command.
-- See http://developer.postgresql.org/pgdocs/postgres/protocol-message-formats.html
setCommandTag :: MonadState Result m => L.ByteString -> m ()
setCommandTag block = do
  modify $ \r -> r { resultTagRows = rows }
    where rows =
            case tag block of
              ["INSERT",_oid,readMay -> Just rows]         -> return rows
              [cmd,readMay -> Just rows] | cmd `elem` cmds -> return rows
              _                                            -> Nothing
          tag = words . concat . map toString . L.toChunks . runGet getString
          cmds = ["DELETE","UPDATE","SELECT","MOVE","FETCH"]


getNotification :: L.ByteString -> Notification
getNotification = runGet parseMsg
  where
    parseMsg = do
       notificationPid     <- fromIntegral `fmap` getInt32
       notificationChannel <- getString
       notificationData    <- getString
       return (Notification{..})


-- | Update the row description of the result.
getRowDesc :: MonadState Result m => Map ObjectId String -> L.ByteString -> m ()
getRowDesc types block =
  modify $ \r -> r {
    resultDesc = Just (parseFields types (runGet parseMsg block))
  }
    where parseMsg = do
            fieldCount :: Int16 <- getInt16
            forM [1..fieldCount] $ \_ -> do
              name <- getString
              objid <- getInt32
              colid <- getInt16
              dtype <- getInt32
              size <- getInt16
              modifier <- getInt32
              code <- getInt16
              return (name,objid,colid,dtype,size,modifier,code)

-- | Parse a row description.
--
-- Parts of the row description are:
--
-- String: The field name.
--
-- Int32: If the field can be identified as a column of a specific
-- table, the object ID of the table; otherwise zero.
--
-- Int16: If the field can be identified as a column of a specific
-- table, the attribute number of the column; otherwise zero.
----
-- Int32: The object ID of the field's data type.
----
-- Int16: The data type size (see pg_type.typlen). Note that negative
-- values denote variable-width types.
----
-- Int32: The type modifier (see pg_attribute.atttypmod). The meaning
-- of the modifier is type-specific.
--
-- Int16: The format code being used for the field. Currently will be
-- zero (text) or one (binary). In a RowDescription returned from the
-- statement variant of Describe, the format code is not yet known and
-- will always be zero.
--
parseFields :: Map ObjectId String
            -> [(L.ByteString,Int32,Int16,Int32,Int16,Int32,Int16)]
            -> [Field]
parseFields types = map parse where
  parse (_fieldName
        ,_ -- parseObjId        -> _objectId
        ,_ -- parseAttrId       -> _attrId
        ,parseType types   -> typ
        ,_ -- parseSize         -> _typeSize
        ,_ -- parseModifier typ -> _typeModifier
        ,parseFormatCode   -> formatCode)
    = Field {
      fieldType = typ
    , fieldFormatCode = formatCode
    }

-- These aren't used (yet).

-- -- | Parse an object ID. 0 means no object.
-- parseObjId :: Int32 -> Maybe ObjectId
-- parseObjId 0 = Nothing
-- parseObjId n = Just (ObjectId n)

-- -- | Parse an attribute ID. 0 means no object.
-- parseAttrId :: Int16 -> Maybe ObjectId
-- parseAttrId 0 = Nothing
-- parseAttrId n = Just (ObjectId $ fromIntegral n)

-- | Parse a number into a type.
parseType :: Map ObjectId String -> Int32 -> Type
parseType types objId =
  case M.lookup (ObjectId objId) types of
    Just name -> case typeFromName name of
                   Just typ -> typ
                   Nothing -> error $ "parseType: Unknown type: " ++ show name
    _ -> error $ "parseType: Unable to parse type: " ++ show objId

typeFromName :: String -> Maybe Type
typeFromName = flip lookup fieldTypes

fieldTypes :: [(String, Type)]
fieldTypes =
  [("bool",Boolean)
  ,("int2",Short)
  ,("integer",Long)
  ,("int",Long)
  ,("int4",Long)
  ,("int8",LongLong)
  ,("timestamptz",TimestampWithZone)
  ,("varchar",CharVarying)
  ,("text",Text)]

-- This isn't used yet.
-- | Parse a type's size.
-- parseSize :: Int16 -> Size
-- parseSize (-1) = Varying
-- parseSize n    = Size n

-- This isn't used yet.
-- -- | Parse a type-specific modifier.
-- parseModifier :: Type -> Int32 -> Maybe Modifier
-- parseModifier _typ _modifier = Nothing

-- | Parse a format code (text or binary).
parseFormatCode :: Int16 -> FormatCode
parseFormatCode 1 = BinaryCode
parseFormatCode _ = TextCode

-- | Add a data row to the response.
getDataRow :: MonadState Result m => L.ByteString -> m ()
getDataRow block =
  modify $ \r -> r { resultRows = runGet parseMsg block : resultRows r }
    where parseMsg = do
            values :: Int16 <- getInt16
            forM [1..values] $ \_ -> do
              size <- getInt32
              if size == -1
                 then return Nothing
                 else do v <- getByteString (fromIntegral size)
                         return (Just v)

-- TODO:
-- getNotice :: MonadState Result m => L.ByteString -> m ()
-- getNotice block =
--   return ()
--  modify $ \r -> r { responseNotices = runGet parseMsg block : responseNotices r }
--    where parseMsg = return ""

typeFromChar :: Char -> Maybe MessageType
typeFromChar c = lookup c types

charFromType :: MessageType -> Maybe Char
charFromType typ = fmap fst $ find ((==typ).snd) types

types :: [(Char, MessageType)]
types = [('C',CommandComplete)
        ,('T',RowDescription)
        ,('D',DataRow)
        ,('I',EmptyQueryResponse)
        ,('E',ErrorResponse)
        ,('Z',ReadyForQuery)
        ,('N',NoticeResponse)
        ,('R',AuthenticationOk)
        ,('Q',Query)
        ,('p',PasswordMessage)]

-- | Blocks until receives ReadyForQuery.
waitForReady :: PGHandle -> IO ()
waitForReady h = loop where
  loop = do
  (typ,block) <- getMessage h
  case typ of
    ErrorResponse -> E.throw $ GeneralError $ show block
    ReadyForQuery | decode block == 'I' -> return ()
    _                                   -> loop

--------------------------------------------------------------------------------
-- Connections

-- | Atomically perform an action with the database handle, if there is one.
withConnection :: Connection -> (PGHandle -> IO a) -> IO a
withConnection Connection{connectionRequest} m = do
    (reqSource,reqSink) <- newEdge
    rspSink <- newEmptyMVar
    rspSource <- Source `fmap` newMVar rspSink
    putMVar connectionRequest (Dialog reqSource rspSink)
    a <- m (PGHandle rspSource reqSink)
    writeSink reqSink Done
    return a

-- | Send a block of bytes on a handle, prepending the message type
--   and complete length.
sendMessage :: PGHandle -> MessageType -> Put -> IO ()
sendMessage h typ output =
  case charFromType typ of
    Just char -> sendBlock h (Just char) output
    Nothing   -> error $ "sendMessage: Bad message type " ++ show typ

-- | Send a block of bytes on a handle, prepending the complete length.
sendBlock :: PGHandle -> Maybe Char -> Put -> IO ()
sendBlock (PGHandle _ sink) typ output = do
  writeSink sink (ReqMsg bytes)
    where bytes = start `mappend` out
          start = runPut $ do
            maybe (return ()) (put . toByte) typ
            int32 $ fromIntegral int32Size +
                    fromIntegral (L.length out)
          out = runPut output
          toByte c = fromIntegral (fromEnum c) :: Word8

-- | Get a message (block) from the stream.
getMessage_ :: Handle -> IO RspMsg
getMessage_ h = do
    messageType <- L.hGet h 1
    blockLength <- L.hGet h int32Size
    if fromIntegral (L.length blockLength) < int32Size
    then eof
    else do
      let typ = decode messageType
          rest = fromIntegral (decode blockLength :: Int32) - int32Size
      block <- L.hGet h rest
      if fromIntegral (L.length block) < rest
      then eof
      else return (RspMsg typ block)
  where
    eof = do
        isEOF <- hIsEOF h
        if isEOF
        then throwIO ConnectionLost
        else fail "getMessage_:  the impossible just happened"

getMessage :: PGHandle -> IO (MessageType,L.ByteString)
getMessage (PGHandle source _) = do
  (RspMsg typ block) <- readSource source
  return (maybe UnknownMessageType id $ typeFromChar typ,block)

--------------------------------------------------------------------------------
-- Binary input/output

-- | Put a Haskell string, encoding it to UTF-8, and null-terminating it.
string :: B.ByteString -> Put
string s = do putByteString s; zero

-- | Put a Haskell 32-bit integer.
int32 :: Int32 -> Put
int32 = put

-- | Put zero-byte terminator.
zero :: Put
zero = put (0 :: Word8)

-- | To avoid magic numbers, size of a 32-bit integer in bytes.
int32Size :: Int
int32Size = 4

getInt16 :: Get Int16
getInt16 = get

getInt32 :: Get Int32
getInt32 = get

getString :: Get L.ByteString
getString = getLazyByteStringNul

readMay :: Read a => String -> Maybe a
readMay x = case reads x of
              [(v,"")] -> return v
              _        -> Nothing
