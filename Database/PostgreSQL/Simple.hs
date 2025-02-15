{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE BangPatterns, DeriveDataTypeable, OverloadedStrings, DisambiguateRecordFields #-}

-- |
-- Module:      Database.PostgreSQL.Simple
-- Copyright:   (c) 2011 Chris Done, 2011 MailRank, Inc.
-- License:     BSD3
-- Maintainer:  Chris Done <chrisdone@gmail.com>
-- Stability:   experimental
-- Portability: portable
--
-- A mid-level client library for the MySQL database, aimed at ease of
-- use and high performance.

module Database.PostgreSQL.Simple
    (
    -- * Writing queries
    -- $use

    -- ** The Query type
    -- $querytype

    -- ** Parameter substitution
    -- $subst

    -- *** Type inference
    -- $inference

    -- ** Substituting a single parameter
    -- $only_param

    -- ** Representing a list of values
    -- $in

    -- ** Modifying multiple rows at once
    -- $many

    -- * Extracting results
    -- $result

    -- ** Handling null values
    -- $null

    -- ** Type conversions
    -- $types

    -- * Types
      ConnectInfo(..)
    , Connection
    , Query
    , In(..)
    , Binary(..)
    , Only(..)
    , ProcessedQuery
    , Pool
    -- ** Exceptions
    , FormatError(fmtMessage, fmtQuery, fmtParams)
    , QueryError(qeMessage, qeQuery)
    , ResultError(errSQLType, errHaskellType, errMessage)
    -- * Connection management
    , Base.connect
    , Base.defaultConnectInfo
    , Base.close
    -- * Queries that return results
    , query
    , query_
    , processQuery
    , queryProcessed
    -- * Queries that stream results
    -- , fold
    -- , fold_
    -- , forEach
    -- , forEach_
    -- * Statements that do not return results
    , execute
    , execute_
    , executeMany
    -- * Transaction handling
    , withTransaction
    , Base.commit
    , Base.rollback
    -- * Helper functions
    , formatMany
    , formatQuery
    ) where

import Blaze.ByteString.Builder (Builder, fromByteString, toByteString)
import Blaze.ByteString.Builder.Char8 (fromChar)
import Control.Applicative ((<$>), pure)
import Control.Exception (Exception, throw)
import Control.Monad (forM)
import Data.ByteString (ByteString)
import Data.List (intersperse)
import Data.Monoid (mappend, mconcat)
import Data.Typeable (Typeable)
import Database.PostgreSQL.Base.Types (ConnectInfo(..),Connection(..),Pool)
import Database.PostgreSQL.Simple.Param (Action(..), inQuotes)
import Database.PostgreSQL.Simple.QueryParams (QueryParams(..))
import Database.PostgreSQL.Simple.QueryResults (QueryResults(..))
import Database.PostgreSQL.Simple.Result (ResultError(..))
import Database.PostgreSQL.Simple.Types (Binary(..), In(..), Only(..), Query(..))
import Text.Regex.PCRE.Light (compile, caseless, match)
import qualified Data.ByteString.Char8 as B
import qualified Database.PostgreSQL.Base as Base
import Data.Monoid

-- | Exception thrown if a 'Query' could not be formatted correctly.
-- This may occur if the number of \'@?@\' characters in the query
-- string does not match the number of parameters provided.
data FormatError = FormatError {
      fmtMessage :: String
    , fmtQuery :: Query
    , fmtParams :: [ByteString]
    } deriving (Eq, Show, Typeable)

instance Exception FormatError

-- | Exception thrown if 'query' is used to perform an @INSERT@-like
-- operation, or 'execute' is used to perform a @SELECT@-like operation.
data QueryError = QueryError {
      qeMessage :: String
    , qeQuery :: Query
    } deriving (Eq, Show, Typeable)

instance Exception QueryError

-- | Format a query string.

-- This function is exposed to help with debugging and logging. Do not
-- use it to prepare queries for execution.
--
-- String parameters are escaped according to the character set in use
-- on the 'Connection'.
--
-- Throws 'FormatError' if the query string could not be formatted
-- correctly.
formatQuery :: QueryParams q => Query -> q -> IO ByteString
formatQuery q@(Query template) qs
    | null xs && '?' `B.notElem` template = return template
    | otherwise = toByteString <$> buildQuery q template xs
  where xs = renderParams qs

-- | Format a query string with a variable number of rows.
--
-- This function is exposed to help with debugging and logging. Do not
-- use it to prepare queries for execution.
--
-- The query string must contain exactly one substitution group,
-- identified by the SQL keyword \"@VALUES@\" (case insensitive)
-- followed by an \"@(@\" character, a series of one or more \"@?@\"
-- characters separated by commas, and a \"@)@\" character. White
-- space in a substitution group is permitted.
--
-- Throws 'FormatError' if the query string could not be formatted
-- correctly.
formatMany :: (QueryParams q) => Query -> [q] -> IO ByteString
formatMany q [] = fmtError "no rows supplied" q []
formatMany q@(Query template) qs = do
  case match re template [] of
    Just [_,before,qbits,after] -> do
      bs <- mapM (buildQuery q qbits . renderParams) qs
      return . toByteString . mconcat $ fromByteString before :
                                        intersperse (fromChar ',') bs ++
                                        [fromByteString after]
    _ -> error "formatMany: The query did not match the documented format."
  where
   re = compile "^([^?]+\\bvalues\\s*)\
                 \(\\(\\s*[?](?:\\s*,\\s*[?])*\\s*\\))\
                 \([^?]*)$"
        [caseless]

buildQuery :: Query -> ByteString -> [Action] -> IO Builder
buildQuery q template xs = zipParams (split template) <$> mapM sub xs
  where sub (Plain b)  = pure b
        sub (Escape s) = pure $ (inQuotes . fromByteString . Base.escapeBS) s
        sub (Many ys)  = mconcat <$> mapM sub ys
        split s = fromByteString h : if B.null t then [] else split (B.tail t)
            where (h,t) = B.break (=='?') s
        zipParams (t:ts) (p:ps) = t `mappend` p `mappend` zipParams ts ps
        zipParams [t] []        = t
        zipParams _ _ = fmtError (show (B.count '?' template) ++
                                  " '?' characters, but " ++
                                  show (length xs) ++ " parameters") q xs

-- | Execute an @INSERT@, @UPDATE@, or other SQL query that is not
-- expected to return results.
--
-- Returns the number of rows affected.
--
-- Throws 'FormatError' if the query could not be formatted correctly.
execute :: (QueryParams q) => Connection -> Query -> q -> IO Integer
execute conn template qs = do
  Base.exec conn =<< formatQuery template qs

-- | A version of 'execute' that does not perform query substitution.
execute_ :: Connection -> Query -> IO Integer
execute_ conn (Query stmt) = do
  Base.exec conn stmt

-- | Execute a multi-row @INSERT@, @UPDATE@, or other SQL query that is not
-- expected to return results.
--
-- Returns the number of rows affected.
--
-- Throws 'FormatError' if the query could not be formatted correctly.
executeMany :: (QueryParams q) => Connection -> Query -> [q] -> IO Integer
executeMany _ _ [] = return 0
executeMany conn q qs = do
  Base.exec conn =<< formatMany q qs

-- | Perform a @SELECT@ or other SQL query that is expected to return
-- results. All results are retrieved and converted before this
-- function returns.
--
-- When processing large results, this function will consume a lot of
-- client-side memory.  Consider using 'fold' instead.
--
-- Exceptions that may be thrown:
--
-- * 'FormatError': the query string could not be formatted correctly.
--
-- * 'QueryError': the result contains no columns (i.e. you should be
--   using 'execute' instead of 'query').
--
-- * 'ResultError': result conversion failed.
query :: (QueryParams q, QueryResults r)
         => Connection -> Query -> q -> IO [r]
query conn template qs = do
  q <- formatQuery template qs
  (fields,rows) <- Base.query conn q
  forM rows $ \row -> let !c = convertResults fields row
                      in return c

-- | A version of 'query' that does not perform query substitution.
query_ :: (QueryResults r) => Connection -> Query -> IO [r]
query_ conn (Query q) = do
  (fields,rows) <- Base.query conn q
  forM rows $ \row -> let !c = convertResults fields row
                      in return c

-- | A processed, appendable. query
newtype ProcessedQuery r = ProcessedQuery ByteString
  deriving (Monoid,Show)

-- | Process a query for later use.
processQuery :: (QueryParams q,QueryResults r) => Query -> q -> IO (ProcessedQuery r)
processQuery template qs = fmap ProcessedQuery $ formatQuery template qs

-- | A version of 'query' that does not perform query substitution.
queryProcessed :: (QueryResults r) => Connection -> ProcessedQuery r -> IO [r]
queryProcessed conn (ProcessedQuery q) = do
  (fields,rows) <- Base.query conn q
  forM rows $ \row -> let !c = convertResults fields row
                      in return c

-- | Execute an action inside a SQL transaction.
--
-- This function initiates a transaction with a \"@begin
-- transaction@\" statement, then executes the supplied action.  If
-- the action succeeds, the transaction will be completed with
-- 'Base.commit' before this function returns.
--
-- If the action throws /any/ kind of exception (not just a
-- MySQL-related exception), the transaction will be rolled back using
-- 'Base.rollback', then the exception will be rethrown.
withTransaction :: Connection -> IO a -> IO a
withTransaction = Base.withTransaction

fmtError :: String -> Query -> [Action] -> a
fmtError msg q xs = throw FormatError {
                      fmtMessage = msg
                    , fmtQuery = q
                    , fmtParams = map twiddle xs
                    }
  where twiddle (Plain b)  = toByteString b
        twiddle (Escape s) = s
        twiddle (Many ys)  = B.concat (map twiddle ys)

-- $use
--
-- SQL-based applications are somewhat notorious for their
-- susceptibility to attacks through the injection of maliciously
-- crafted data. The primary reason for widespread vulnerability to
-- SQL injections is that many applications are sloppy in handling
-- user data when constructing SQL queries.
--
-- This library provides a 'Query' type and a parameter substitution
-- facility to address both ease of use and security.

-- $querytype
-- 
-- A 'Query' is a @newtype@-wrapped 'ByteString'. It intentionally
-- exposes a tiny API that is not compatible with the 'ByteString'
-- API; this makes it difficult to construct queries from fragments of
-- strings.  The 'query' and 'execute' functions require queries to be
-- of type 'Query'.
--
-- To most easily construct a query, enable GHC's @OverloadedStrings@
-- language extension and write your query as a normal literal string.
--
-- > {-# LANGUAGE OverloadedStrings #-}
-- >
-- > import Database.MySQL.Simple
-- >
-- > hello = do
-- >   conn <- connect defaultConnectInfo
-- >   query conn "select 2 + 2"
--
-- A 'Query' value does not represent the actual query that will be
-- executed, but is a template for constructing the final query.

-- $subst
--
-- Since applications need to be able to construct queries with
-- parameters that change, this library provides a query substitution
-- capability.
--
-- The 'Query' template accepted by 'query' and 'execute' can contain
-- any number of \"@?@\" characters.  Both 'query' and 'execute'
-- accept a third argument, typically a tuple. When constructing the
-- real query to execute, these functions replace the first \"@?@\" in
-- the template with the first element of the tuple, the second
-- \"@?@\" with the second element, and so on. If necessary, each
-- tuple element will be quoted and escaped prior to substitution;
-- this defeats the single most common injection vector for malicious
-- data.
--
-- For example, given the following 'Query' template:
--
-- > select * from user where first_name = ? and age > ?
--
-- And a tuple of this form:
--
-- > ("Boris" :: String, 37 :: Int)
--
-- The query to be executed will look like this after substitution:
--
-- > select * from user where first_name = 'Boris' and age > 37
--
-- If there is a mismatch between the number of \"@?@\" characters in
-- your template and the number of elements in your tuple, a
-- 'FormatError' will be thrown.
--
-- Note that the substitution functions do not attempt to parse or
-- validate your query. It's up to you to write syntactically valid
-- SQL, and to ensure that each \"@?@\" in your query template is
-- matched with the right tuple element.

-- $inference
--
-- Automated type inference means that you will often be able to avoid
-- supplying explicit type signatures for the elements of a tuple.
-- However, sometimes the compiler will not be able to infer your
-- types. Consider a care where you write a numeric literal in a
-- parameter tuple:
--
-- > query conn "select ? + ?" (40,2)
--
-- The above query will be rejected by the compiler, because it does
-- not know the specific numeric types of the literals @40@ and @2@.
-- This is easily fixed:
--
-- > query conn "select ? + ?" (40 :: Double, 2 :: Double)
--
-- The same kind of problem can arise with string literals if you have
-- the @OverloadedStrings@ language extension enabled.  Again, just
-- use an explicit type signature if this happens.

-- $only_param
--
-- Haskell lacks a single-element tuple type, so if you have just one
-- value you want substituted into a query, what should you do?
--
-- The obvious approach would appear to be something like this:
--
-- > instance (Param a) => QueryParam a where
-- >     ...
--
-- Unfortunately, this wreaks havoc with type inference, so we take a
-- different tack. To represent a single value @val@ as a parameter, write
-- a singleton list @[val]@, use 'Just' @val@, or use 'Only' @val@.
--
-- Here's an example using a singleton list:
--
-- > execute conn "insert into users (first_name) values (?)"
-- >              ["Nuala"]

-- $in
--
-- Suppose you want to write a query using an @IN@ clause:
--
-- > select * from users where first_name in ('Anna', 'Boris', 'Carla')
--
-- In such cases, it's common for both the elements and length of the
-- list after the @IN@ keyword to vary from query to query.
--
-- To address this case, use the 'In' type wrapper, and use a single
-- \"@?@\" character to represent the list.  Omit the parentheses
-- around the list; these will be added for you.
--
-- Here's an example:
--
-- > query conn "select * from users where first_name in ?" $
-- >       In ["Anna", "Boris", "Carla"]
--
-- If your 'In'-wrapped list is empty, the string @\"(null)\"@ will be
-- substituted instead, to ensure that your clause remains
-- syntactically valid.

-- $many
--
-- If you know that you have many rows of data to insert into a table,
-- it is much more efficient to perform all the insertions in a single
-- multi-row @INSERT@ statement than individually.
--
-- The 'executeMany' function is intended specifically for helping
-- with multi-row @INSERT@ and @UPDATE@ statements. Its rules for
-- query substitution are different than those for 'execute'.
--
-- What 'executeMany' searches for in your 'Query' template is a
-- single substring of the form:
--
-- > values (?,?,?)
--
-- The rules are as follows:
--
-- * The keyword @VALUES@ is matched case insensitively.
--
-- * There must be no other \"@?@\" characters anywhere in your
--   template.
--
-- * There must one or more \"@?@\" in the parentheses.
--
-- * Extra white space is fine.
--
-- The last argument to 'executeMany' is a list of parameter
-- tuples. These will be substituted into the query where the @(?,?)@
-- string appears, in a form suitable for use in a multi-row @INSERT@
-- or @UPDATE@.
--
-- Here is an example:
--
-- > executeMany conn
-- >   "insert into users (first_name,last_name) values (?,?)"
-- >   [("Boris","Karloff"),("Ed","Wood")]
--
-- The query that will be executed here will look like this
-- (reformatted for tidiness):
--
-- > insert into users (first_name,last_name) values
-- >   ('Boris','Karloff'),('Ed','Wood')

-- $result
--
-- The 'query' and 'query_' functions return a list of values in the
-- 'QueryResults' typeclass. This class performs automatic extraction
-- and type conversion of rows from a query result.
--
-- Here is a simple example of how to extract results:
--
-- > import qualified Data.Text as Text
-- >
-- > xs <- query_ conn "select name,age from users"
-- > forM_ xs $ \(name,age) ->
-- >   putStrLn $ Text.unpack name ++ " is " ++ show (age :: Int)
--
-- Notice two important details about this code:
--
-- * The number of columns we ask for in the query template must
--   exactly match the number of elements we specify in a row of the
--   result tuple.  If they do not match, a 'ResultError' exception
--   will be thrown.
--
-- * Sometimes, the compiler needs our help in specifying types. It
--   can infer that @name@ must be a 'Text', due to our use of the
--   @unpack@ function. However, we have to tell it the type of @age@,
--   as it has no other information to determine the exact type.

-- $null
--
-- The type of a result tuple will look something like this:
--
-- > (Text, Int, Int)
--
-- Although SQL can accommodate @NULL@ as a value for any of these
-- types, Haskell cannot. If your result contains columns that may be
-- @NULL@, be sure that you use 'Maybe' in those positions of of your
-- tuple.
--
-- > (Text, Maybe Int, Int)
--
-- If 'query' encounters a @NULL@ in a row where the corresponding
-- Haskell type is not 'Maybe', it will throw a 'ResultError'
-- exception.

-- $only_result
--
-- To specify that a query returns a single-column result, use the
-- 'Only' type.
--
-- > xs <- query_ conn "select id from users"
-- > forM_ xs $ \(Only dbid) -> {- ... -}

-- $types
--
-- Conversion of SQL values to Haskell values is somewhat
-- permissive. Here are the rules.
--
-- * For numeric types, any Haskell type that can accurately represent
--   all values of the given MySQL type is considered \"compatible\".
--   For instance, you can always extract a MySQL @TINYINT@ column to
--   a Haskell 'Int'.  The Haskell 'Float' type can accurately
--   represent MySQL integer types of size up to @INT24@, so it is
--   considered compatble with those types.
--
-- * A numeric compatibility check is based only on the type of a
--   column, /not/ on its values. For instance, a MySQL @LONG_LONG@
--   column will be considered incompatible with a Haskell 'Int8',
--   even if it contains the value @1@.
--
-- * If a numeric incompatibility is found, 'query' will throw a
--   'ResultError'.
--
-- * The 'String' and 'Text' types are assumed to be encoded as
--   UTF-8. If you use some other encoding, decoding may fail or give
--   wrong results. In such cases, write a @newtype@ wrapper and a
--   custom 'Result' instance to handle your encoding.
