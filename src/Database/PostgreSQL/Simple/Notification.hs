-----------------------------------------------------------------------------
-- |
-- Module      :  Database.PostgreSQL.Simple.Notification
-- Copyright   :  (c) 2011 Leon P Smith
-- License     :  BSD3
--
-- Maintainer  :  Leon P Smith <leon@melding-monads.com>
--
-- Asynchronous notification support for PostgreSQL
--
-----------------------------------------------------------------------------

module Database.PostgreSQL.Simple.Notification
     ( -- * Asynchronous Notifications
       Notification(..)
     , NotificationChannel
     , getNotificationChannel
     , dupNotificationChannel
     , getNotification
     ) where

import Control.Concurrent.Edge
import Database.PostgreSQL.Base.Types

getNotificationChannel :: Connection -> IO NotificationChannel
getNotificationChannel conn = do
    NotificationChannel conn `fmap` getSource (connectionNotification conn)

dupNotificationChannel :: NotificationChannel -> IO NotificationChannel
dupNotificationChannel (NotificationChannel conn source) = do
    NotificationChannel conn `fmap` dupSource source

getNotification :: NotificationChannel -> IO Notification
getNotification (NotificationChannel _conn source) = do
    readSource source
