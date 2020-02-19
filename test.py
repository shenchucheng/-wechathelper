import logging
import traceback
import threading
import time
import io
import os
import sys
import sqlite3
from itchat import Core
from itchat import content, utils, config
from itchat.components.login import push_login
from itchat.components.register import logger, Queue, templates, set_logging, test_connect
from functools import partial
from sqlhelper import sqlitehelper


class Bot(Core):
    def __init__(self, **kwargs):
        super().__init__()
        self.filehelper = self.self = templates.User
        self.db: sqlite3.Connection = None
        self.cursor: sqlite3.Cursor = None
        self.errorMsgList = []
        for key in self.functionDict.keys():
            __msgDict = {}.fromkeys(content.INCOME_MSG)
            for _ in __msgDict.keys():
                __msgDict[_] = {}
            self.functionDict[key] = __msgDict
        self.setting = {
            "database": {"dir": "", "table_info": {}},
            "dir": {"workDir": ".",
                    "dataDir": 'data',
                    "tempDir": os.path.join("data", "temp"),
                    "mediaDir": os.path.join("data", "media")},
        }
        if kwargs.get("dir"):
            dir_update = kwargs.pop("dir")
            _ = dir_update.get("workDir")
            if _:
                if not os.path.exists(_):
                    os.makedirs(_)
                os.chdir(_)
                logger.info("Create work dir successfully")
                dir_update.pop("workDir")
            self.setting["dir"].update(dir_update)
        self.setting.update(kwargs)
        for path in self.setting["dir"].values():
            if not os.path.exists(path):
                os.makedirs(path)
                logger.info("Make dir {} successfully".format(os.path.abspath(path)))
        self.sf_init()

    def login(self, enableCmdQR=False, picDir=None, qrCallback=None,
              loginCallback=None, exitCallback=None):
        if self.alive or self.isLogging:
            logger.warning('itchat has already logged in.')
            return
        picDir = (picDir or os.path.join(self.setting["dir"]["tempDir"], "QR.png"))
        exitCallback = (exitCallback or self.exit_callback)
        self.isLogging = True
        while self.isLogging:
            uuid = push_login(self)
            if uuid:
                qrStorage = io.BytesIO()
            else:
                logger.info('Getting uuid of QR code.')
                while not self.get_QRuuid():
                    time.sleep(1)
                logger.info('Downloading QR code.')
                qrStorage = self.get_QR(enableCmdQR=enableCmdQR,
                                        picDir=picDir, qrCallback=qrCallback)
                logger.info('Please scan the QR code to log in.')
            isLoggedIn = False
            while not isLoggedIn:
                status = self.check_login()
                if hasattr(qrCallback, '__call__'):
                    qrCallback(uuid=self.uuid, status=status, qrcode=qrStorage.getvalue())
                if status == '200':
                    isLoggedIn = True
                elif status == '201':
                    if isLoggedIn is not None:
                        logger.info('Please press confirm on your phone.')
                        isLoggedIn = None
                elif status != '408':
                    break
            if isLoggedIn:
                break
            elif self.isLogging:
                logger.info('Log in time out, reloading QR code.')
        else:
            return  # log in process is stopped by user
        logger.info('Loading the contact, this may take a little while.')
        self.web_init()
        self.show_mobile_login()
        self.get_contact(True)
        if hasattr(loginCallback, '__call__'):
            if os.path.exists(picDir or config.DEFAULT_QR):
                os.remove(picDir or config.DEFAULT_QR)
            r = loginCallback()
        else:
            utils.clear_screen()
            if os.path.exists(picDir or config.DEFAULT_QR):
                os.remove(picDir or config.DEFAULT_QR)
            logger.info('Login successfully as %s' % self.storageClass.nickName)
        self.start_receiving(exitCallback)
        self.isLogging = False

    def auto_login(self, hotReload=True, statusStorageDir=None,
                   enableCmdQR=False, picDir=None, qrCallback=None,
                   loginCallback=None, exitCallback=None):
        if not test_connect():
            logger.info("You can't get access to internet or wechat domain, so exit.")
            sys.exit()
        loginCallback = (loginCallback or self.login_callback)
        exitCallback = (exitCallback or self.exit_callback)
        statusStorageDir = (statusStorageDir or os.path.join(self.setting["dir"]["dataDir"], "login.pkl"))
        self.useHotReload = hotReload
        self.hotReloadDir = statusStorageDir
        if hotReload:
            if self.load_login_status(statusStorageDir,
                                      loginCallback=loginCallback, exitCallback=exitCallback):
                return
            self.login(enableCmdQR=enableCmdQR, picDir=picDir, qrCallback=qrCallback,
                       loginCallback=loginCallback, exitCallback=exitCallback)
            self.dump_login_status(statusStorageDir)
        else:
            self.login(enableCmdQR=enableCmdQR, picDir=picDir, qrCallback=qrCallback,
                       loginCallback=loginCallback, exitCallback=exitCallback)

    def msg_register(self, msgType, isFriendChat=False,
                     isGroupChat=False, isMpChat=False, fType="rf"):
        if not (isinstance(msgType, list) or isinstance(msgType, tuple)):
            msgType = [msgType]

        def _msg_register(fn):
            for _msgType in msgType:
                if isFriendChat:
                    self.functionDict['FriendChat'][_msgType][fType] = fn
                if isGroupChat:
                    self.functionDict['GroupChat'][_msgType][fType] = fn
                if isMpChat:
                    self.functionDict['MpChat'][_msgType][fType] = fn
                if not any((isFriendChat, isGroupChat, isMpChat)):
                    self.functionDict['FriendChat'][_msgType][fType] = fn
            return fn

        return _msg_register
            
    def sf_init(self):
        register = partial(self.msg_register, fType="sf")

        def re_format(t, l):
            _time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))
            logger.info('{}: {}'.format(_time, l))

        def re_wrapper(fn):
            def _re(msg):
                _id, _time = msg.msgId, msg.createTime
                user = msg.user
                _user = (user.get("RemarkName") or user.get("NickName") or user.get("UserName"))
                _type, _content, comments = fn(msg)
                if isinstance(msg['User'], (templates.User, templates.MassivePlatform)):
                    _from = 1 if msg.fromUserName == self.storageClass.userName else 2
                    _log = "{} {} {} message {}".format(["Send to", "Receive from"][_from - 1],
                                                        _user, msg.type, (_content or ''))
                elif isinstance(msg['User'], templates.Chatroom):
                    _from = msg.actualNickName
                    _log = "{} send {} message {} at {}".format(_from, msg.type,
                                                                (_content or ''), _user)
                    _from = str((msg.actualUserName, _from))
                else:
                    raise NotImplementedError(msg)
                re_format(_time, _log)
                return None, (_id, _time, _user, _from, _type, _content, comments)
            return _re

        @register(content.TEXT, isFriendChat=True, isMpChat=True, isGroupChat=True)
        @re_wrapper
        def _fn(msg):
            return 1, msg.content, None

        def process_pic(msg):
            _type = msg.msgType
            if _type == 47:
                if msg.hasProductId:
                    _content = "#store#"
                    comments = None
                    _type = 21  # emoji store
                else:
                    _content = "#emoji#"
                    comments = msg.content
                    _type = 22  # emoji
            elif _type == 49:
                _content = "#favorites#"  # msg.mediaId
                comments = msg.content
                _type = 23
            elif _type == 3:
                _content = "#Pic"
                comments = None
            else:
                raise NotImplementedError(msg)
            return _type, _content, comments

        @register(content.PICTURE, isFriendChat=True, isMpChat=True, isGroupChat=True)
        @re_wrapper
        def _fn(msg):
            return process_pic(msg)

        def sys_wrapper(fn):
            def _re(msg):
                _time = (msg.get("CreateTime", round(time.time())))
                _infoType, _name, _log = fn(msg)
                _sql = "INSERT INTO SystemMsgs VALUES (?, ?, ?)"
                re_format(_time, _log)
                self.cursor(_sql, (_time, _infoType, _name))
            return _re

        @register(content.SYSTEM)
        @sys_wrapper
        def _fn(msg):
            if msg.systemInfo == 'uins':
                user = msg.user
                if user.userName == self.storageClass.userName:
                    infoType = 1  # open the app or scan on the phone
                    name = None
                    log = 'Using app on the phone'
                else:
                    infoType = 2  # open the dialog with sb.
                    name = user.get("RemarkName", user.userName)
                    log = "open the dialog with {} on the phone".format(name)
            elif msg.systemInfo == 'chatrooms':
                name = None
                if msg.text:
                    infoType = 5  # GroupChat operation
                    log = "GroupChat operation probably"
                else:
                    infoType = 6  # Set pinned operation probably
                    log = "Set pinned operation probably"
            else:
                raise NotImplementedError(msg)
            return infoType, name, log

        @register(content.SYSTEM, isMpChat=True, isGroupChat=True)
        @sys_wrapper
        def _fn(msg):
            if msg.systemInfo == 'uins':
                name = msg.user.nickName
                if isinstance(msg['User'], templates.MassivePlatform):
                    infoType = 4  # open the MassivePlatform dialog .
                elif isinstance(msg['User'], templates.Chatroom):
                    infoType = 5  # open the chatroom dialog .
                else:
                    raise NotImplementedError(msg)
                log = "open the dialog with {} on the phone".format(name)
            else:
                raise NotImplementedError(msg)
            return infoType, name, log

    def configured_reply(self):
        try:
            msg = self.msgList.get(timeout=2)
        except Queue.Empty:
            self.db.commit()
        else:
            if isinstance(msg['User'], templates.User):
                replyFn = self.functionDict['FriendChat'].get(msg['Type'])
                _table = "FriendMsgs"
            elif isinstance(msg['User'], templates.MassivePlatform):
                replyFn = self.functionDict['MpChat'].get(msg['Type'])
                _table = "MpMsgs"
            elif isinstance(msg['User'], templates.Chatroom):
                replyFn = self.functionDict['GroupChat'].get(msg['Type'])
                _table = "GroupMsgs"
            saveFn = replyFn.get('sf')
            replyFn = replyFn.get('rf')
            if saveFn:
                try:
                    _sql = "INSERT INTO {} VALUES (?, ?, ?, ?, ?, ?, ?)".format(_table)
                    sql, args = saveFn(msg)
                    sql = sql or _sql
                    self.cursor.execute(sql, args)
                except:
                    info = traceback.format_exc()
                    self.errorMsgList.append((msg, info))
                    logger.warning(info)

            if replyFn:
                try:
                    r = replyFn(msg)
                    if r is not None:
                        self.send(r, msg.get('FromUserName'))
                except:
                    logger.warning(traceback.format_exc())

    def run(self, debug=False, blockThread=True):
        logger.info('Start auto replying.')
        if debug:
            set_logging(loggingLevel=logging.DEBUG)

        def reply_fn():
            try:
                while self.alive:
                    self.configured_reply()
            except KeyboardInterrupt:
                if self.useHotReload:
                    self.dump_login_status()
                self.alive = False
                logger.debug('itchat received an ^C and exit.')
                logger.info('Bye~')

        if blockThread:
            reply_fn()
        else:
            replyThread = threading.Thread(target=reply_fn)
            replyThread.setDaemon(True)
            replyThread.start()

    def login_callback(self):
        utils.clear_screen()
        if self.storageClass.nickName == '':
            logger.warning('Fail to login, if use auto_login with "hotReload" ,\
            please delete the "pkl"("statusStorageDir") and try login again')
            sys.exit()
        self.filehelper = self.update_friend(userName='filehelper')
        self.self = self.update_friend(userName=self.storageClass.userName)
        self.db, self.cursor = db_init(self.self.uin,
                                       (self.setting["database"]["dir"] or self.setting["dir"]["dataDir"]),
                                       self.setting["database"]["table_info"])
        self.cursor.row_factory = sqlite3.Row
        self.cursor.execute(
            sqlitehelper.select(
                "User",
                ("datetime(LoginTime,'unixepoch','localtime')",
                 "LoginTime"),
                ("UserName", "NickName")),
            (self.storageClass.userName, self.storageClass.nickName)
        )
        row = self.cursor.fetchone()
        if row:
            logger.info('Welcome to wechathelper! First login at {} keeping alive in {}'.format(
                row[0], time_length(time.time() - row["LoginTime"])))
        else:
            self.cursor.execute(
                sqlitehelper.insert("User", 4),
                (self.storageClass.userName, self.storageClass.nickName,
                 round(time.time()), None)
            )
            logger.info('Login initialization successfully and starting listening messages'.format(
                self.storageClass.nickName))
            self.db.commit()

    def exit_callback(self):
        now = round(time.time())
        self.cursor.execute("UPDATE User SET LogoutTime = ? where userName = ?", (now, self.storageClass.userName))
        self.cursor.execute(
            sqlitehelper.select("User", "logoutTime - LoginTime", {"userName": self.storageClass.userName}))
        _ = self.cursor.fetchone()[0]
        self.db.commit()
        self.db.close()
        logger.info("Logout! Online in {} at this login periods".format(time_length(_)))


def time_length(length):
    unis = ["Seconds", "Minutes"]
    for uni in unis:
        if length > 60:
            length = length / 60
        else:
            return ' '.join((str(round(length, 2)), uni))
    return ' '.join((str(round(length, 2)), "Hours"))


def db_init(uin, db_dir='', table_info=None):
    """
    :param uin: user's uin
    :param db_dir: the database dir
    :param table_info: add table to initialize
    :return: database connection
    """
    table_info_default = {
        "User": {
            "columns": ("UserName CHAR(65) NOT NULL",
                        "NickName VARCHAR NOT NULL",
                        "LoginTime INT(10) NOT NULL",
                        "LogoutTime INT(10)"),
            "unique": ("UserName", "LoginTime")
        },
        "Friends": {
            "columns": ("UserId INT(4) NOT NULL",
                        "NickName VARCHAR",
                        "RemarkName VARCHAR",
                        "Province CHAR(3)",
                        "City CHAR(3)",
                        "Sex INT(1)",
                        "StarFriend INT(1)",
                        "AttrStatus INT",
                        "SnsFlag INT",
                        "ContactFlag INT",
                        "HeadImgUrl VARCHAR",
                        "History VARCHAR"),  # 历史更改
            "primary_key": "UserId"
        },
        "Groups": {
            "columns": ("NickName VARCHAR",
                        "IsOwner INT(1)",
                        "ContactFlag INT",
                        "MemberCount INT",
                        "HeadImgUrl VARCHAR",
                        "MemberList VARCHAR",
                        "History VARCHAR"),
        },
        "Mps": {
            "columns": ("NickName VARCHAR",
                        "Province CHAR(3)",
                        "City CHAR(3)",
                        "SnsFlag INT",
                        "ContactFlag INT",
                        "HeadImgUrl VARCHAR",
                        "History VARCHAR"),
            "primary_key": "NickName"
        },
        "FriendMsgs": {
            "columns": ("MsgId NUMERIC NOT NULL",
                        "CreateTime INT(10) NOT NULL",
                        # "UserId INT(4) NOT NULL",
                        "User VARCHAR",
                        "FromUser INT(1) --0: Bot; 1: Self; 2: User\n",
                        "MsgType INT(2)",
                        "Content BLOG",
                        "Comments TEXT"),
            "primary_key": ("MsgId", "CreateTime")
        },
        "GroupMsgs": {
            "columns": ("MsgId NUMERIC NOT NULL",
                        "CreateTime INT(10) NOT NULL",
                        "ChatRoom VARCHAR",
                        "FromUser VARCHAR",
                        "MsgType INT(2)",
                        "Content BLOG",
                        "Comments TEXT"),
            "primary_key": ("MsgId", "CreateTime")
        },
        "MpMsgs": {
            "columns": ("MsgId NUMERIC NOT NULL",
                        "CreateTime INT(10) NOT NULL",
                        "NickName VARCHAR",
                        "FromUser INT(1) --0: Bot; 1: Self;2: User\n",
                        "MsgType INT(2)",
                        "Content BLOG",
                        "Comments TEXT"),
            "primary_key": ("MsgId", "CreateTime")
        },
        "SystemMsgs": {
            "columns": ("CreateTime INT(10) NOT NULL",
                        "Type INT(1) NOT NULL--0:; 1:; 2:;\n",
                        "Comments TEXT"),
        },
        "MediaMsgs": {
            "columns": ("Md5 CHAR(16)",
                        "Type VARCHAR",
                        "Content VARCHAR",
                        "Comment VARCHAR",
                        "FileName VARCHAR"),
            "primary_key": ()
        },
    }
    if table_info is not None:
        table_info_default.update(table_info)
    table_info = table_info_default
    db_name = str(uin) + '.db'
    db = sqlite3.connect(os.path.join(db_dir, db_name), check_same_thread=False)
    cursor = db.execute(sqlitehelper.show_tables)
    tables = set()
    for _ in cursor:
        table, = _
        tables.add(table)
    for table, info in table_info.items():
        if table not in tables:
            cursor.execute(
                sqlitehelper.create_table(
                    table,
                    info["columns"],
                    unique=info.get("unique"),
                    primary_key=info.get("primary_key")
                )
            )
            logger.info("Create table {} in database {} successfully".format(table, db_name))
    logger.info("Database initialise for user {} successfully".format(uin))
    db.commit()
    return db, cursor


if __name__ == '__main__':
    bot = Bot()
    bot.auto_login()
    bot.run(blockThread=1)
