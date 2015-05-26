/* redisclient.h -- a C++ client library for redis.
 *
 * Copyright (c) 2009, Brian Hammond <brian at fictorial dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef REDISCLIENT_H
#define REDISCLIENT_H

#include <errno.h>
typedef unsigned long ulong;
#define REDISCLIENT_UNUSED(x) x
#ifdef _WIN32
#  define NOMINMAX
#  include <WinSock2.h>
#  define INFINITY 0x7fffffff
typedef int ssize_t;
#else
#  include <sys/socket.h>
#endif


#if defined(_WIN32) || defined(__ANDROID__)
typedef unsigned short ushort;
typedef unsigned long ulong;
typedef unsigned int uint;
typedef char* BUFPTR;
#else
typedef void* BUFPTR;
#endif


#include <string>
#include <vector>
#include <map>
#include <set>
#include <list>
#include <stdexcept>
#include <ctime>
#include <sstream>

#include <boost/concept_check.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/functional/hash.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/random.hpp>
#include <boost/cstdint.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/optional.hpp>
#include <boost/variant.hpp>
#include <boost/signals.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>

#ifdef _WIN32
#include "anet_win32.h"
#else
#include "anet.h"
#include "poll.h"
#endif

#define REDIS_LBR                       "\r\n"
#define REDIS_STATUS_REPLY_OK           "OK"
#define REDIS_PREFIX_STATUS_REPLY_ERROR "-ERR "
#define REDIS_PREFIX_STATUS_REPLY_ERR_C '-'
#define REDIS_PREFIX_STATUS_REPLY_VALUE '+'
#define REDIS_PREFIX_SINGLE_BULK_REPLY  '$'
#define REDIS_NULL_BULK_REPLY           "$-1"
#define REDIS_PREFIX_MULTI_BULK_REPLY   '*'
#define REDIS_NULL_MULTI_BULK_REPLY     "*-1"
#define REDIS_PREFIX_INT_REPLY          ':'
#define REDIS_WHITESPACE                " \f\n\r\t\v"
#define REDIS_PREFIX_STATUS_REPLY_REDIRECT "-MOVED "
#define REDIS_CLUSTER_SLOT_NUM          (16384)

template<class Object >
struct make;
namespace redis
{
  template<typename CONSISTENT_HASHER>
  class base_client;

  enum reply_t
  {
    no_reply,
    status_code_reply,
    error_reply,
    int_reply,
    bulk_reply,
    multi_bulk_reply,
    moved_reply
  };

  struct connection_wrapper
  {
	  virtual int connect(char *err, char *addr, int port) = 0;
	  virtual int setnodelay(char *err, int fd) = 0;
	  virtual int send(int fd, char *buf, int count) = 0;
	  virtual int recv(int fd, BUFPTR buf, int count, int flags) = 0;
  };

  struct connection_data
  {
    connection_data(const std::string & host = "localhost", uint16_t port = 6379, const std::string & password = "", int dbindex = 0)
     : host(host), password(password), port(port), dbindex(dbindex), socket(ANET_ERR)
    {
    }

    bool operator==(const connection_data & other) const
    {
      if(host != other.host)
        return false;
      if(port != other.port)
        return false;
      if(dbindex != other.dbindex)
        return false;
      if(password != other.password)
        return false;

      return true;
    }

    std::string host;
    std::string password;
    std::string ident; // a string in the form: "{host}:{port}"
    boost::uint16_t port;
    int dbindex;

  private:
    int socket;

    template<typename CONSISTENT_HASHER>
    friend class base_client;
  };

  enum server_role
  {
    role_master,
    role_slave
  };

  // Generic error that is thrown when communicating with the redis server.

  class redis_error : public std::exception
  {
  public:
    redis_error(const std::string & err) : err_(err) {}
    virtual ~redis_error() throw () {}
    operator const std::string () const { return err_; }
    virtual const char* what() const throw ()
    {
      return err_.c_str();
    }

  private:
    std::string err_;
  };

  // Some socket-level I/O or general connection error.

  class connection_error : public redis_error
  {
  public:
    connection_error(const std::string & err) : redis_error(err) {}
  };

  // Redis gave us a reply we were not expecting.
  // Possibly an internal error (here or in redis, probably here).

  class protocol_error : public redis_error
  {
  public:
    protocol_error(const std::string & err) : redis_error(err) {};
  };

  // A key that you expected to exist does not in fact exist.

  class key_error : public redis_error
  {
  public:
    key_error(const std::string & err) : redis_error(err) {};
  };

  // A operation with time limit does not deliver an result early enough.

  class timeout_error : public redis_error
  {
  public:
    timeout_error(const std::string & err) : redis_error(err) {};
  };

  // A value of an expected type or other semantics was found to be invalid.

  class value_error : public redis_error
  {
  public:
    value_error(const std::string & err) : redis_error(err) {};
  };

  struct key
  {
    explicit key(const std::string & name)
    : name(name)
    {
    }

    std::string name;
  };

  class makecmd
  {
  public:
    explicit makecmd(const std::string & cmd_name)
    {
      append(cmd_name);
      //if (!finalize)
      //  buffer_ << " ";
    }

    const std::string & key_name() const
    {
      if(!key_name_)
        throw std::runtime_error("No key defined!");
      return *key_name_;
    }

    inline makecmd & operator<<(const key & datum)
    {
      if(key_name_)
        throw std::runtime_error("You could not add a second key");
      else
        key_name_ = datum.name;
      append(datum.name);
      return *this;
    }

    inline makecmd & operator<<(const std::string & datum)
    {
      append(datum);
      return *this;
    }

    template <typename T>
    makecmd & operator<<(T const & datum)
    {
      append( boost::lexical_cast<std::string>(datum) );
      return *this;
    }

    makecmd & operator<<(const std::vector<std::string> & data)
    {
      lines_.insert( lines_.end(), data.begin(), data.end() );
      return *this;
    }

    template <typename T>
    makecmd & operator<<(const std::vector<T> & data)
    {
      size_t n = data.size();
      for (size_t i = 0; i < n; ++i)
      {
        append( boost::lexical_cast<std::string>( data[i] ) );
        //if (i < n - 1)
        //  buffer_ << " ";
      }
      return *this;
    }

    operator std::string () const
    {
      std::ostringstream oss;
      size_t n = lines_.size();
      oss << REDIS_PREFIX_MULTI_BULK_REPLY << n << REDIS_LBR;

      for (size_t i = 0; i < n; ++i)
      {
        const std::string & param = lines_[i];
        oss << REDIS_PREFIX_SINGLE_BULK_REPLY << param.size() << REDIS_LBR;
        oss << param << REDIS_LBR;
      }

      return oss.str();
    }

  private:
    void append(const std::string & param)
    {
      lines_.push_back(param);
    }

    std::vector<std::string> lines_;
    boost::optional<std::string> key_name_;
  };

  template<typename CONSISTENT_HASHER>
  class base_client;

  typedef boost::variant< std::string, int, std::vector<std::string> > reply_val_t;
  typedef std::pair<reply_t, reply_val_t> reply_data_t;

  class command
  {
  private:
    std::string request_;
    std::string hash_key_;
    reply_data_t reply_;

    void check_reply_t(reply_t reply_type) const
    {
      if( reply_.first != reply_type )
        throw std::runtime_error("invalid reply type");
    }

    void set_reply(const reply_data_t & reply)
    {
      reply_ = reply;
    }

    template<typename CONSISTENT_HASHER>
    friend class base_client;

  public:
    command( const makecmd & cmd_input )
    : request_(cmd_input), hash_key_(cmd_input.key_name())
    {
	   	//std::cout<<"request_="<<request_<<std::endl;
		//std::cout<<"hash_key_="<<hash_key_<<std::endl;	 
      reply_.first = no_reply;
    }

    reply_t reply_type() const
    {
      return reply_.first;
    }

    const std::string & get_status_code_reply() const
    {
      check_reply_t(status_code_reply);
      return boost::get<std::string>(reply_.second);
    }

    const std::string & get_error_reply() const
    {
      check_reply_t(error_reply);
      return boost::get<std::string>(reply_.second);
    }

    int get_int_reply() const
    {
      check_reply_t(int_reply);
      return boost::get<int>(reply_.second);
    }

    const std::string & get_bulk_reply() const
    {
      check_reply_t(bulk_reply);
      return boost::get<std::string>(reply_.second);
    }

    const std::vector<std::string> & get_multi_bulk_reply() const
    {
      check_reply_t(multi_bulk_reply);
      return boost::get< std::vector<std::string> >(reply_.second);
    }
  };

  struct server_info
  {
    std::string version;
    bool bgsave_in_progress;
    unsigned long connected_clients;
    unsigned long connected_slaves;
    unsigned long used_memory;
    unsigned long changes_since_last_save;
    unsigned long last_save_time;
    unsigned long total_connections_received;
    unsigned long total_commands_processed;
    unsigned long uptime_in_seconds;
    unsigned long uptime_in_days;
    server_role role;
    unsigned short arch_bits;
    std::string multiplexing_api;
    std::map<std::string, std::string> param_map;
  };

  // You should construct a 'client' object per connection to a redis-server.
  //
  // Please read the online redis command reference:
  // http://code.google.com/p/redis/wiki/CommandReference
  //
  // No provisions for customizing the allocator on the string/bulk value type
  // (std::string) are provided.  If needed, you can always change the
  // string_type typedef in your local version.

  template<typename CONSISTENT_HASHER>
  class base_client
  {
  private:
    void init(connection_data & con)
    {
      char err[ANET_ERR_LEN];
      con.socket = (conn_wrapper)
		  ? conn_wrapper->connect(err, const_cast<char*>(con.host.c_str()), con.port)
		  : anetTcpConnect(err, const_cast<char*>(con.host.c_str()), con.port);
      if (con.socket == ANET_ERR)
      {
        std::ostringstream os;
        os << err << " (redis://" << con.host << ':' << con.port << ")";
        throw connection_error( os.str() );
      }
      
	  (conn_wrapper)
		  ? conn_wrapper->setnodelay(NULL, con.socket)
		  : anetTcpNoDelay(NULL, con.socket);

	  if (!con.password.empty()) {
         send_(con.socket, makecmd("AUTH") << con.password);
         recv_ok_reply_(con.socket);
	  }
      select(con.dbindex, con);

      char ipbuf[256];
      if (ANET_ERR == anetResolve(err, (char*)(con.host.c_str()), ipbuf))
      {
        throw connection_error("Failed on host name resolving.");
      }

      std::ostringstream os;
      os << ipbuf << ':' << con.port;
      con.ident = os.str();
    }

	void errSubscibeMode( const std::string cmd ) {
		std::cerr << "This is subscribe mode now, can't use command " << cmd << ", sorry!" << std::endl;
	}

  public:
    typedef std::string string_type;
    typedef std::vector<string_type> string_vector;
    typedef std::pair<string_type, string_type> string_pair;
    typedef std::vector<string_pair> string_pair_vector;
    typedef std::pair<string_type, double> string_score_pair;
    typedef std::vector<string_score_pair> string_score_vector;
    typedef std::set<string_type> string_set;

    typedef long int_type;

    explicit base_client(const string_type & host = "localhost",
                    uint16_t port = 6379, const string_type & password = "", int_type dbindex = 0,
					connection_wrapper *wrapper = 0)
    {
      connection_data con;
      con.host = host;
      con.port = port;
      con.dbindex = dbindex;
      con.password = password;
	  subscribe_mode = false;
	  unsubscribing = true;
	  conn_wrapper = wrapper;
	  m_pThread = NULL;
      init(con);
      connections_.push_back(con);
      connident_map_.insert(std::make_pair(con.ident, (unsigned int)(connections_.size() - 1)));
      memset(slotmap, 0, sizeof(unsigned int)*REDIS_CLUSTER_SLOT_NUM);
    }

    template<typename CON_ITERATOR>
    base_client(CON_ITERATOR begin, CON_ITERATOR end)
    {
	  unsubscribing = true;
      subscribe_mode = false;
	  conn_wrapper = NULL;
	  m_pThread = NULL;
	  
      while(begin != end)
      {
        connection_data con = *begin;
        init(con);
        connections_.push_back(con);
        connident_map_.insert(std::make_pair(con.ident, (unsigned int)(connections_.size() - 1)));
        begin++;
      }
      memset(slotmap, 0, sizeof(unsigned int)*REDIS_CLUSTER_SLOT_NUM);

      if( connections_.empty() )
        throw std::runtime_error("No connections given!");
    }

    base_client<CONSISTENT_HASHER>* clone() const
    {
      return new base_client<CONSISTENT_HASHER>(connections_.begin(), connections_.end());
    }

    inline static string_type missing_value()
    {
      return "**nonexistent-key**";
    }

    enum datatype
    {
      datatype_none,      // key doesn't exist
      datatype_string,
      datatype_list,
      datatype_set,
      datatype_zset,
      datatype_hash,
      datatype_unknown
    };

	connection_wrapper* get_connection_wrapper() const
	{
	  return conn_wrapper;
	}

	void set_connection_wrapper(connection_wrapper *wrapper)
	{
		conn_wrapper = wrapper;
	}

    int_type get_list(const string_type & key, string_vector & out)
    {
      return lrange(key, 0, -1, out);
    }

    void lrem_exact(const string_type & key,
                    int_type count,
                    const string_type & value)
    {
      if (lrem(key, count, value) != count)
        throw value_error("failed to remove exactly N elements from list");
    }
    enum range_specifier
    {
      exclude_min = 1 << 0,
      exclude_max = 1 << 1
    };

    enum aggregate_type
    {
      aggregate_sum = 1 << 0,
      aggregate_min = 1 << 1,
      aggregate_max = 1 << 2
    };

    enum sort_order
    {
      sort_order_ascending,
      sort_order_descending
    };

    ~base_client()
    {
		// region Add by adu

	  	unsubscribing = true;
		if( m_pThread ) {
			m_pThread->join();
			delete m_pThread;
			m_pThread = NULL;
		}

		m_channel2Signal.clear();
		// endregion

      BOOST_FOREACH(connection_data & con, connections_)
      {
        if (con.socket != ANET_ERR)
#ifdef _WIN32
          closesocket((SOCKET)con.socket);
#else
          close(con.socket);
#endif
      }
    }

    const std::vector<connection_data> & connections() const
    {
      return connections_;
    }

    void auth(const string_type & pass)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "auth" );
			return;
		}
	
      if( connections_.size() > 1 )
        throw std::runtime_error("feature is not available in cluster mode");

      int socket = connections_[0].socket;
      send_(socket, makecmd("AUTH") << pass);
      recv_ok_reply_(socket);
    }

    void set(const string_type & key,
                          const string_type & value)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "set" );
			return;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("SET") << key << value);
	  try{
      recv_ok_reply_(socket);
	  	}
	  catch(redis::protocol_error r){
	  	std::cout << r.what() << std::endl;
	  	}
    }

    // Matt: Returns CROSSSLOT error when mixing keys with different slot.
    void mset( const string_vector & keys, const string_vector & values )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "mset" );
			return;
		}
		
      assert( keys.size() == values.size() );

      std::map< int, boost::optional<makecmd> > socket_commands;

      for(size_t i=0; i < keys.size(); i++)
      {
        int socket = get_socket(keys);
        boost::optional<makecmd> & cmd = socket_commands[socket];
        if(!cmd)
          cmd = makecmd("MSET");
        *cmd << keys[i] << values[i];
      }

      typedef std::pair< int, boost::optional<makecmd> > sock_pair;
      BOOST_FOREACH(const sock_pair & sp, socket_commands)
      {
        send_(sp.first, *sp.second);
      }

      BOOST_FOREACH(const sock_pair & sp, socket_commands)
      {
        recv_ok_reply_(sp.first);
      }
    }

    // Matt: Returns CROSSSLOT error when mixing keys with different slot.
    void mset( const string_pair_vector & key_value_pairs )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "mset" );
			return;
		}
		
      std::map< int, boost::optional<makecmd> > socket_commands;

      for(size_t i=0; i < key_value_pairs.size(); i++)
      {
        const string_type & key = key_value_pairs[i].first;
        const string_type & value = key_value_pairs[i].second;

        int socket = get_socket(key);
        boost::optional<makecmd> & cmd = socket_commands[socket];
        if(!cmd)
          cmd = makecmd("MSET");
        *cmd << key << value;
      }

      typedef std::pair< int, boost::optional<makecmd> > sock_pair;
      BOOST_FOREACH(const sock_pair & sp, socket_commands)
      {
        send_(sp.first, *sp.second);
      }

      BOOST_FOREACH(const sock_pair & sp, socket_commands)
      {
        recv_ok_reply_(sp.first);
      }
    }

  private:
    struct msetex_data
    {
      boost::optional<makecmd> mset_cmd;
      std::string expire_cmds;
      size_t count;
    };

  public:
    // Matt: Returns CROSSSLOT error when mixing keys with different slot.
    void msetex( const string_pair_vector & key_value_pairs, int_type seconds )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "msetex" );
			return;
		}
		
      std::map< int, msetex_data > socket_commands;

      for(size_t i=0; i < key_value_pairs.size(); i++)
      {
        const string_type & key = key_value_pairs[i].first;
        const string_type & value = key_value_pairs[i].second;

        int socket = get_socket(key);
        msetex_data & dat = socket_commands[socket];
        boost::optional<makecmd> & cmd = dat.mset_cmd;
        if(!cmd)
        {
          cmd = makecmd("MSET");
          dat.count = 0;
        }
        *cmd << key << value;

        std::string & expire_cmds = dat.expire_cmds;
        expire_cmds += makecmd("EXPIRE") << key << seconds;
        dat.count++;
      }

      typedef std::pair< int, msetex_data > sock_pair;
      BOOST_FOREACH(const sock_pair & sp, socket_commands)
      {
        std::string cmds = *sp.second.mset_cmd;
        cmds += sp.second.expire_cmds;
        send_(sp.first, cmds);
      }

      BOOST_FOREACH(const sock_pair & sp, socket_commands)
      {
        recv_ok_reply_(sp.first);
        for(size_t i= 0; i < sp.second.count; i++)
          recv_int_ok_reply_(sp.first);

      }
    }

    string_type get(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "get" );
			return missing_value();
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("GET") << key);
      return recv_bulk_reply_(socket);
    }

    string_type getset(const string_type & key, const string_type & value)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "getset" );
			return missing_value();
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("GETSET") << key << value);
      return recv_bulk_reply_(socket);
    }

  private:
    struct connection_keys
    {
      boost::optional<makecmd> cmd;

      /// Gives the position of the value in the original array
      std::vector<size_t> indices;
    };

    void exec_during_subscribing(command & cmd)
    {
      int socket = get_socket(cmd.hash_key_);
      send_( socket, cmd.request_ );
    }
	
  public:
    void exec(command & cmd)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "exec" );
			return;
		}
		
      int socket = get_socket(cmd.hash_key_);
      send_( socket, cmd.request_ );
      reply_data_t reply = recv_generic_reply_(socket);
      if (reply.first == moved_reply)
      {
        int rsock = handle_redirect_errmsg(socket, boost::get<std::string>(reply.second));
        assert(rsock != socket);
        send_(rsock, peek_cmd_(socket));
        reply = recv_generic_reply_(rsock);
        assert(reply.first != moved_reply);
        pop_cmd_(socket);
      }
      cmd.set_reply( reply );
    }

    // Matt: It is ok to use keys having different key slots in a single exec batch.
    //       Note: Atomic operation of a single batch is not guaranteed.
    void exec(std::vector<command> & commands)
    {
      if( true == subscribe_mode ) {
        errSubscibeMode( "exec" );
        return;
      }

      //std::map< int, std::string > socket_commands;
      std::vector<int> sockets;
      sockets.reserve(commands.size());
      for(size_t i=0; i < commands.size(); i++)
      {
        int socket = get_socket( commands[i].hash_key_ );
        //socket_commands[socket] += commands[i].request_;
        send_(socket, commands[i].request_);
        sockets.push_back(socket);
      }

      //typedef std::pair< int, std::string > sock_pair;
      //BOOST_FOREACH(const sock_pair & sp, socket_commands)
      //{
      //  send_(sp.first, sp.second);
      //}

      bool redirected = false;
      for (size_t i=0; i < commands.size(); i++)
      {
        effective_socket = sockets[i];
        commands[i].set_reply( recv_generic_reply_(effective_socket) );
        if (commands[i].reply_type() == moved_reply)
          redirected = true;
      }

      if (!redirected)
        return;

      // Handle the redirections ...
      // Step 1/2: Re-send requests to redirected socket.
      for (size_t i = 0; i < commands.size(); i++)
      {
        if (commands[i].reply_type() != moved_reply)
          continue;
        int socket = sockets[i];
        int rsock = handle_redirect_errmsg(socket, boost::get<std::string>(commands[i].reply_.second));
        assert(rsock != socket);
        send_(rsock, peek_cmd_(socket)); // Re-send the same request to the redirected socket.
        pop_cmd_(socket);
        sockets[i] = rsock;
      }

      // Step 2/2: Handle the responses in the same order as sending-out.
      for (size_t i = 0; i < commands.size(); i++)
      {
        if (commands[i].reply_type() != moved_reply)
          continue;
        commands[i].set_reply(recv_generic_reply_(sockets[i]));
        assert(commands[i].reply_type() != moved_reply ); // Note: Since we've been redirected once, there should not be any further redirection.
      }
    }

    // Matt: Throw exception if 2 or more keys in transaction batch have difference key slot.
    //       Note: This behaviour make this redisclient version not comply to Redis 2.x
    bool exec_transaction(std::vector<command> & commands)
    {
      if( true == subscribe_mode ) {
        errSubscibeMode( "exec_transaction" );
        return false;
      }

      int cmd_socket = -1;
      int redirected_socket = -1;
      unsigned short target_slot = 0xFFFF;
      //std::string cmd_str = makecmd("MULTI");
	  
      for(size_t i=0; i < commands.size(); i++)
      {		  
        unsigned short slotidx = hasher_(commands[i].hash_key_, static_cast<const std::vector<connection_data> &>(connections_));
		std::cout << "hash key: " << commands[i].hash_key_ << " slot idx: " << slotidx << std::endl;
        if (target_slot == 0xFFFF)
          target_slot = slotidx;
        if (target_slot != slotidx)
		{
			std::cout << "OMG1!!!!" << std::endl;
          throw std::runtime_error("calls in transaction map to different key slot!");
		}

        int socket = get_socket( commands[i].hash_key_ );
        if (cmd_socket == -1)
        {
          cmd_socket = socket;
		  std::cout << "send multi" << std::endl;
          send_(cmd_socket, makecmd("MULTI"));
        }
        else if (cmd_socket != socket)
        {
			std::cout << "OMG!!!!" << std::endl;
          throw std::runtime_error("calls in transaction map to different socket!");
        }

        //cmd_str += commands[i].request_;
        send_(cmd_socket, commands[i].request_);
      }

      //cmd_str += makecmd("EXEC");
      send_(cmd_socket, makecmd("EXEC"));

      //send_(cmd_socket, cmd_str);
      recv_ok_reply_(cmd_socket); // MULTI => +OK
      for(size_t i=0; i < commands.size(); i++)
      {
        std::string resp = read_line(cmd_socket);
        if (resp != "+QUEUED")
        {
          int rsock = handle_redirect_errmsg(cmd_socket, resp);
          if (rsock != cmd_socket)
          {
            // This transaction must happen on another server instance.
            redirected_socket = rsock;
          }
          else
          {
            throw std::runtime_error("invalid state (expected 'QUEUED' or '-MOVED ' in transaction but got '" + resp + "')");
          }
        }
      }

      std::string line = read_line(cmd_socket); // Response of EXEC
	  std::cout << line << std::endl;
	  
      // Transaction has failed because of redirection is needed.
      // Retry 1 more time shoud be OK since the redirected socket has been registered.
      if ((line[0] == REDIS_PREFIX_STATUS_REPLY_ERR_C) && (redirected_socket != -1))
      {
        pop_all_cmds_(cmd_socket);
        return exec_transaction(commands);
      }

      if ( line[0] != REDIS_PREFIX_MULTI_BULK_REPLY )
        throw std::runtime_error("EXEC does not return a multi bulk reply");

      // Matt: When using WATCH, EXEC can return a Null multi-bulk reply if the execution was aborted.
      if (line.substr(0, 3) == REDIS_NULL_MULTI_BULK_REPLY)
      {
        pop_all_cmds_(cmd_socket);
        return false;
      }

      for(size_t i=0; i < commands.size(); i++)
      { // Since the involved keys are all with the same hash slot, no redirection should happen.
        commands[i].set_reply( recv_generic_reply_(cmd_socket) );
      }
      pop_cmd_(cmd_socket); // pop cmd of EXEC
      return true;
    }

    // Matt: Returns CROSSSLOT error when mixing keys with different slot.
    void mget(const string_vector & keys, string_vector & out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "mget" );
			return;
		}
		
      out = string_vector( keys.size() );
      std::map< int, connection_keys > socket_commands;

      for(size_t i=0; i < keys.size(); i++)
      {
        int socket = get_socket(keys[i]);
        connection_keys & con_keys = socket_commands[socket];
        boost::optional<makecmd> & cmd = con_keys.cmd;
        if(!cmd)
          cmd = makecmd("MGET");
        *cmd << keys[i];
        con_keys.indices.push_back(i);
      }

      typedef std::pair< int, connection_keys > sock_pair;
      BOOST_FOREACH(const sock_pair & sp, socket_commands)
      {
        send_(sp.first, *sp.second.cmd);
      }

      BOOST_FOREACH(const sock_pair & sp, socket_commands)
      {
        const connection_keys & con_keys = sp.second;
        string_vector cur_out;
        recv_multi_bulk_reply_(sp.first, cur_out);

        for(size_t i=0; i < cur_out.size(); i++)
          out[con_keys.indices[i]] = cur_out[i];
      }
    }

    bool setnx(const string_type & key,
                            const string_type & value)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "setnx" );
			return false;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("SETNX") << key << value);
      return recv_int_reply_(socket) == 1;
    }

    // Matt: Returns CROSSSLOT error when mixing keys with different slot.
    bool msetnx( const string_vector & keys, const string_vector & values )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "msetnx" );
			return false;
		}
		
      assert( keys.size() == values.size() );

      std::map< int, boost::optional<makecmd> > socket_commands;

      for(size_t i=0; i < keys.size(); i++)
      {
        int socket = get_socket(keys);
        boost::optional<makecmd> & cmd = socket_commands[socket];
        if(!cmd)
          cmd = makecmd("MSETNX");
        *cmd << keys[i] << values[i];
      }

      if( socket_commands.size() > 1 )
        throw std::runtime_error("feature is not available in cluster mode");

      typedef std::pair< int, boost::optional<makecmd> > sock_pair;
      BOOST_FOREACH(const sock_pair & sp, socket_commands)
      {
        send_(sp.first, *sp.second);
        recv_ok_reply_(sp.first);
      }
    }

    // Matt: Returns CROSSSLOT error when mixing keys with different slot.
    bool msetnx( const string_pair_vector & key_value_pairs )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "msetnx" );
			return false;
		}
		
      std::map< int, boost::optional<makecmd> > socket_commands;

      for(size_t i=0; i < key_value_pairs.size(); i++)
      {
        int socket = get_socket("");
        boost::optional<makecmd> & cmd = socket_commands[socket];
        if(!cmd)
          cmd = makecmd("MSETNX");
        *cmd << key_value_pairs[i].first << key_value_pairs[i].second;
      }

      if( socket_commands.size() > 1 )
        throw std::runtime_error("feature is not available in cluster mode");

      typedef std::pair< int, boost::optional<makecmd> > sock_pair;
      BOOST_FOREACH(const sock_pair & sp, socket_commands)
      {
        send_(sp.first, *sp.second);
        recv_ok_reply_(sp.first);
      }
    }

    void setex(const string_type & key, const string_type & value, unsigned int secs)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "setex" );
			return;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("SETEX") << key << secs << value);
      recv_ok_reply_(socket);
    }

    size_t append(const string_type & key, const string_type & value)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "append" );
			return (size_t)(boost::lexical_cast<int>(-1));
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("APPEND") << key << value);
      int res = recv_int_reply_(socket);
      if(res < 0)
        throw protocol_error("expected value size");

      assert( static_cast<size_t>(res) >= value.size() );
      return static_cast<size_t>(res);
    }

    string_type substr(const string_type & key, int start, int end)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "substr" );
			return missing_value();
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("SUBSTR") << key << start << end);
      return recv_bulk_reply_(socket);
    }

    int_type incr(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "incr" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("INCR") << key);
      return recv_int_reply_(socket);
    }

    template<typename INT_TYPE>
    INT_TYPE incr(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "incr" );
			return boost::lexical_cast<INT_TYPE>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("INCR") << key);
      return recv_int_reply_<INT_TYPE>(socket);
    }

    int_type incrby(const string_type & key, int_type by)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "incrby" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("INCRBY") << key << by);
      return recv_int_reply_(socket);
    }

    template<typename INT_TYPE>
    INT_TYPE incrby(const string_type & key, INT_TYPE by)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "incrby" );
			return boost::lexical_cast<INT_TYPE>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("INCRBY") << key << by);
      return recv_int_reply_<INT_TYPE>(socket);
    }

    int_type decr(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "decr" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("DECR") << key);
      return recv_int_reply_(socket);
    }

    template<typename INT_TYPE>
    INT_TYPE decr(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "decr" );
			return boost::lexical_cast<INT_TYPE>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("DECR") << key);
      return recv_int_reply_<INT_TYPE>(socket);
    }

    int_type decrby(const string_type & key, int_type by)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "decrby" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("DECRBY") << key << by);
      return recv_int_reply_(socket);
    }

    template<typename INT_TYPE>
    INT_TYPE decrby(const string_type & key, INT_TYPE by)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "decrby" );
			return boost::lexical_cast<INT_TYPE>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("DECRBY") << key << by);
      return recv_int_reply_<INT_TYPE>(socket);
    }

    bool exists(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "exists" );
			return false;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("EXISTS") << key);
      return recv_int_reply_(socket) == 1;
    }

    bool del(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "del" );
			return false;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("DEL") << key);
      return recv_int_reply_(socket) != 0;
    }

    template<typename ITERATOR>
    bool del(ITERATOR begin, ITERATOR end)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "del" );
			return false;
		}
		
      std::map<int, string_vector> sock_key_map;
      while( begin != end )
      {
        string_type key = *begin++;
        sock_key_map[ get_socket(key) ].push_back(key);
      }

      typedef std::pair<const int, string_vector> sock_key_pair;
      BOOST_FOREACH(const sock_key_pair & p, sock_key_map)
      {
        send_(p.first, makecmd("DEL") << p.second);
      }

      int_type res =  false;

      BOOST_FOREACH(const sock_key_pair & p, sock_key_map)
      {
        res += recv_int_reply_(p.first);
      }

      return res;
    }

    datatype type(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "type" );
			return datatype_none;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("TYPE") << key);
      std::string response = recv_single_line_reply_(socket);

      if(response == "none")   return datatype_none;
      if(response == "string") return datatype_string;
      if(response == "list")   return datatype_list;
      if(response == "set")    return datatype_set;
      if(response == "zset")   return datatype_zset;
      if(response == "hash")   return datatype_hash;

#ifndef NDEBUG
      std::cerr << "Got unknown datatype name: " << response << std::endl;
#endif // NDEBUG

      return datatype_unknown;
    }

    int_type keys(const string_type & pattern, string_vector & out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "keys" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      BOOST_FOREACH(const connection_data & con, connections_)
      {
        send_(con.socket, makecmd("KEYS") << pattern);
      }

      int_type res = 0;

      BOOST_FOREACH(const connection_data & con, connections_)
      {
        res += recv_multi_bulk_reply_(con.socket, out);
      }

      return res;
    }

    string_type randomkey()
    {
		if( true == subscribe_mode ) {
 			errSubscibeMode( "randomkey" );
 			return missing_value();
		}
		
      int socket = connections_[0].socket;
      if( connections_.size() > 1 )
      {
        /*
         * Select a random server if there are more then one
         */

        boost::mt19937 gen;
        boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
        boost::posix_time::ptime epoch( boost::gregorian::date(1970, 1, 1) );
        gen.seed( (now-epoch).total_seconds() );

        boost::uniform_int<> dist(0, connections_.size()-1);
        boost::variate_generator< boost::mt19937&, boost::uniform_int<> > die(gen, dist);
        socket = connections_[die()].socket;
      }

      send_(socket, makecmd("RANDOMKEY") );
      return recv_bulk_reply_(socket);
    }

    /**
     * @warning Not cluster save (the old name and the new one must be on the same redis server)
     */
    void rename(const string_type & old_name, const string_type & new_name)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "rename" );
			return;
		}
		
      int source_socket = get_socket(old_name);
      int destin_socket = get_socket(new_name);
      if( source_socket != destin_socket )
      {
        switch( type(old_name) )
        {
          case datatype_none:      // key doesn't exist
            return;
          case datatype_string:
            set(new_name, get(old_name));
            break;
          case datatype_list:
          {
            string_vector content;
            lrange(old_name, 0, -1, content);
            del(new_name);
            BOOST_FOREACH(const string_type & val, content)
            {
              rpush(new_name, val);
            }
            break;
          }
          case datatype_set:
          {
            string_set content;
            smembers(old_name, content);
            del(new_name);
            BOOST_FOREACH(const string_type & val, content)
            {
              sadd(new_name, val);
            }
            break;
          }
          case datatype_zset:
          case datatype_hash:
          case datatype_unknown:
          default:
            throw std::runtime_error("renaming is not supported for this datatype in cluster mode");
        }
        del(old_name);
        return;
      }

      send_(source_socket, makecmd("RENAME") << old_name << new_name);
      recv_ok_reply_(source_socket);
    }

    /**
     * @warning Not cluster save (the old name and the new one must be on the same redis server)
     */
    bool renamenx(const string_type & old_name, const string_type & new_name)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "renamenx" );
			return false;
		}
		
      int source_socket = get_socket(old_name);
      int destin_socket = get_socket(new_name);

      if( source_socket != destin_socket )
      {
        if( exists(new_name) )
          return false;
        rename(old_name, new_name);
        return true;
      }

      send_(source_socket, makecmd("RENAMENX") << old_name << new_name);
      return recv_int_reply_(source_socket) == 1;
    }

    /**
     * @returns the number of keys in the currently selected database. In cluster mode the number
     * of keys in all currently selected databases is returned.
     */
    int_type dbsize()
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "dbsize" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int_type val = 0;

      BOOST_FOREACH(const connection_data & con, connections_)
      {
        send_(con.socket, makecmd("DBSIZE"));
      }

      BOOST_FOREACH(const connection_data & con, connections_)
      {
        val += recv_int_reply_(con.socket);
      }

      return val;
    }

    /**
     * @returns the number of keys in the currently selected database with the given connection.
     */
    int_type dbsize(const connection_data & con)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "dbsize" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      send_(con.socket, makecmd("DBSIZE"));
      return recv_int_reply_(con.socket);
    }

    bool expire(const string_type & key, unsigned int secs)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "expire" );
			return false;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("EXPIRE") << key << secs);
      return (recv_int_reply_(socket) == 1);
    }

    int ttl(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "ttl" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("TTL") << key);
      return recv_int_reply_(socket);
    }

    int_type rpush(const string_type & key,
                            const string_type & value)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "rpush" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("RPUSH") << key << value);
      return recv_int_reply_(socket);
    }

    int_type lpush(const string_type & key,
                            const string_type & value)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "lpush" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("LPUSH") << key << value);
      return recv_int_reply_(socket);
    }

    int_type llen(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "llen" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("LLEN") << key);
      return recv_int_reply_(socket);
    }

    int_type lrange(const string_type & key,
                    int_type start,
                    int_type end,
                    string_vector & out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "lrange" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("LRANGE") << key << start << end);
      return recv_multi_bulk_reply_(socket, out);
    }

    void ltrim(const string_type & key,
                            int_type start,
                            int_type end)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "ltrim" );
			return;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("LTRIM") << key << start << end);
      recv_ok_reply_(socket);
    }

    string_type lindex(const string_type & key,
                                                 int_type index)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "lindex" );
			return missing_value();
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("LINDEX") << key << index);
      return recv_bulk_reply_(socket);
    }

    void lset(const string_type & key, int_type index, const string_type & value)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "lset" );
			return;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("LSET") << key << index << value);
      recv_ok_reply_(socket);
    }

    int_type lrem(const string_type & key, int_type count, const string_type & value)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "lrem" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("LREM") << key << count << value);
      return recv_int_reply_(socket);
    }

    string_type lpop(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "lpop" );
			return missing_value();
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("LPOP") << key);
      return recv_bulk_reply_(socket);
    }

    string_type rpop(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "rpop" );
			return missing_value();
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("RPOP") << key);
      return recv_bulk_reply_(socket);
    }

    /**
     * @warning Not cluster save (all keys must be on the same redis server)
     */
    string_pair blpop(const string_vector & keys, int_type timeout_seconds = 0)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "blpop" );
			return make_pair( missing_value(), missing_value() );
		}
		
      int socket = get_socket(keys);
      if(socket == -1)
        // How to do in cluster mode? Is reinserting of to much poped values a solution?
        throw std::runtime_error("feature is not available in cluster mode");

      send_(socket, makecmd("BLPOP") << keys << timeout_seconds);
      string_vector sv;
      try
      {
        recv_multi_bulk_reply_(socket, sv);
      }
      catch(key_error & e)
      {
        assert(timeout_seconds > 0);
        throw timeout_error("could not pop value in time");
      }
      if(sv.size() == 2)
        return make_pair( sv[0], sv[1] );
      else
        return make_pair( "", missing_value() );
    }

    string_type blpop(const string_type & key, int_type timeout_seconds = 0)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "blpop" );
			return missing_value();
		}
		
      int socket = get_socket(key);
      if(socket == -1)
        // How to do in cluster mode? Is reinserting of to much poped values a solution?
        throw std::runtime_error("feature is not available in cluster mode");

      send_(socket, makecmd("BLPOP") << key << timeout_seconds);
      string_vector sv;
      try
      {
        recv_multi_bulk_reply_(socket, sv);
      }
      catch(key_error & e)
      {
        REDISCLIENT_UNUSED(e);
        assert(timeout_seconds > 0);
        return missing_value(); // should we throw a timeout_error?
                                // we set a timeout so we expect that this can happen
      }
      if(sv.size() == 2)
      {
        assert(key == sv[0]);
        return sv[1];
      }
      else
        return missing_value();
    }

    /**
     * @warning Not cluster save (all keys must be on the same redis server)
     */
    string_pair brpop(const string_vector & keys, int_type timeout_seconds)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "brpop" );
			return make_pair( missing_value(), missing_value() );
		}
		
      int socket = get_socket(keys);
      makecmd m("BRPOP");
      for(size_t i=0; i < keys.size(); i++)
        m << keys[i];
      m << timeout_seconds;
      send_(socket, m);
      string_vector sv;
      try
      {
        recv_multi_bulk_reply_(socket, sv);
      }
      catch(key_error & e)
      {
        assert(timeout_seconds > 0);
          return make_pair("", missing_value() );//missing_value(); // should we throw a timeout_error?
                                // we set a timeout so we expect that this can happen
      }
      if(sv.size() == 2)
        return make_pair( sv[0], sv[1] );
      else
        return make_pair( "", missing_value() );
    }

    string_type brpop(const string_type & key, int_type timeout_seconds)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "brpop" );
			return missing_value();
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("BRPOP") << key << timeout_seconds);
      string_vector sv;
      try
      {
        recv_multi_bulk_reply_(socket, sv);
      }
      catch(key_error & e)
      {
        REDISCLIENT_UNUSED(e);
        assert(timeout_seconds > 0);
        return missing_value(); // should we throw a timeout_error?
                                // we set a timeout so we expect that this can happen
      }
      if(sv.size() == 2)
      {
        assert(key == sv[0]);
        return sv[1];
      }
      else
        return missing_value();
    }

    bool sadd(const string_type & key,
                           const string_type & value)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "sadd" );
			return false;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("SADD") << key << value);
      return recv_int_reply_(socket) == 1;
    }

    template<typename ITERATOR>
    int_type sadd(const string_type & key, ITERATOR begin, ITERATOR end)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "sadd" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      std::string commands;
      size_t count = 0;

      while(begin != end)
      {
        string_type val = *begin++;
        commands += makecmd("SADD") << key << val;
        count++;
      }

      send_(socket, commands);

      int_type res = 0;
      for(size_t i=0; i < count; i++)
      {
        res += recv_int_reply_(socket);
      }
      return res;
    }

    bool srem(const string_type & key,
                           const string_type & value)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "srem" );
			return 0;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("SREM") << key << value);
      return (recv_int_reply_(socket) == 1);
    }

    string_type spop(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "spop" );
			return missing_value();
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("SPOP") << key);
      return recv_bulk_reply_(socket);
    }

    void smove(const string_type & srckey, const string_type & dstkey, const string_type & member)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "smove" );
			return;
		}
		
      int src_socket = get_socket(srckey);
      int dst_socket = get_socket(dstkey);
      if(dst_socket != src_socket)
      {
        srem(srckey, member);
        sadd(dstkey, member);
        return;
      }

      send_(src_socket, makecmd("SMOVE") << srckey << dstkey << member);
      recv_int_ok_reply_(src_socket);
    }

    int_type scard(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "scard" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("SCARD") << key);
      return recv_int_reply_(socket);
    }

    bool sismember(const string_type & key,
                                const string_type & value)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "sismember" );
			return false;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("SISMEMBER") << key << value);
      return recv_int_reply_(socket) == 1;
    }

    /**
     * @returns the intersection between the Sets stored at key1, key2, ..., keyN
     * @warning Not cluster save (all keys must be on the same redis server)
     */
    int_type sinter(const string_vector & keys, string_set & out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "sinter" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      std::map<int, string_vector> per_server;
      BOOST_FOREACH(const string_type & key, keys)
      {
        per_server[get_socket(key)].push_back(key);
      }

      size_t i = 0;

      typedef std::pair<const int, string_vector> per_server_pair;
      BOOST_FOREACH(const per_server_pair & p, per_server)
      {
        send_(p.first, makecmd("SINTER") << p.second);
      }

      BOOST_FOREACH(const per_server_pair & p, per_server)
      {
        string_set cur;
        recv_multi_bulk_reply_(p.first, cur);
        if(i > 0)
        {
          string_set prev = out;
          out.clear();

          std::set_intersection(prev.begin(), prev.end(), cur.begin(), cur.end(), std::inserter(out, out.end()));
        }
        else
        {
          out = cur;
        }
        i++;
      }

      return out.size();
    }

    /**
     * @warning Not cluster save (all keys must be on the same redis server)
     */
    int_type sinterstore(const string_type & dstkey, const string_vector & keys)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "sinterstore" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(dstkey);
      int source_sockets = get_socket(keys);
      if(socket != source_sockets)
      {
        std::cerr << 0 << std::endl;
        string_set content;
        std::cerr << 1 << std::endl;
        sinter(keys, content);
        std::cerr << 2 << std::endl;
        del(dstkey);
        std::cerr << 3 << std::endl;
        BOOST_FOREACH(const string_type & val, content)
        {
          std::cerr << 4 << std::endl;
          sadd(dstkey, val);
        }
        std::cerr << 5 << std::endl;
        return content.size();
      }

      send_(socket, makecmd("SINTERSTORE") << dstkey << keys);
      return recv_int_reply_(socket);
    }

    int_type sunion(const string_vector & keys, string_set & out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "sunion" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(keys);
      if(socket == -1)
      {
        std::map< int, boost::optional<makecmd> > per_socket_keys;
        BOOST_FOREACH(const string_type & key, keys)
        {
          boost::optional<makecmd> & optCmd = per_socket_keys[ get_socket(key) ];
          if( !optCmd )
            optCmd = makecmd("SUNION");
          *optCmd << key;
        }

        typedef std::pair< const int, boost::optional<makecmd> > per_sock_pair;
        BOOST_FOREACH(const per_sock_pair & p, per_socket_keys)
        {
          send_(p.first, *p.second);
        }

        BOOST_FOREACH(const per_sock_pair & p, per_socket_keys)
        {
          recv_multi_bulk_reply_(p.first, out);
        }
        return out.size();
      }

      send_(socket, makecmd("SUNION") << keys);
      return recv_multi_bulk_reply_(socket, out);
    }

    int_type sunionstore(const string_type & dstkey,
                                                   const string_vector & keys)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "sunionstore" );
			return boost::lexical_cast<int_type>(-1);
		}
		
		int socket = get_socket(dstkey);
		int source_sockets = get_socket(keys);
		if(socket != source_sockets)
		{
			string_set content;
			sunion(keys, content);
			del(dstkey);
			return sadd(dstkey, content.begin(), content.end());
		}

		send_(socket, makecmd("SUNIONSTORE") << dstkey << keys);
		return recv_int_reply_(socket);
    }

    int_type sdiff(const string_vector & keys, string_set & out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "sdiff" );
			return boost::lexical_cast<int_type>(-1);
		}
		
		int socket = get_socket(keys);
		send_(socket, makecmd("SDIFF") << keys);
		return recv_multi_bulk_reply_(socket, out);
    }

    int_type sdiffstore(const string_type & dstkey, const string_vector & keys)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "sdiffstore" );
			return boost::lexical_cast<int_type>(-1);
		}

		int socket = get_socket(dstkey);
		int source_sockets = get_socket(keys);
		if(socket != source_sockets)
		throw std::runtime_error("not available in cluster mode");

		send_(socket, makecmd("SDIFFSTORE") << dstkey << keys);
		return recv_int_reply_(socket);
    }

    int_type smembers(const string_type & key, string_set & out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "smembers" );
			return boost::lexical_cast<int_type>(-1);
		}
		
		int socket = get_socket(key);
		send_(socket, makecmd("SMEMBERS") << key);
		return recv_multi_bulk_reply_(socket, out);
    }

    string_type srandmember(const string_type & key)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "srandmember" );
			return missing_value();
		}
		
		int socket = get_socket(key);
		send_(socket, makecmd("SRANDMEMBER") << key);
		return recv_bulk_reply_(socket);
    }

	// since Redis 2.6
    int_type srandmember(const string_type & key, int count, string_vector &out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "srandmember" );
			return boost::lexical_cast<int_type>(-1);
		}
		
		int socket = get_socket(key);
		send_(socket, makecmd("SRANDMEMBER") << key << count);
		return recv_multi_bulk_reply_(socket, out);
    }

	bool time(int &seconds, int &microseconds)
	{
		if( true == subscribe_mode ) {
			errSubscibeMode( "time" );
			return false;
		}
		
		int socket = get_socket("");
		send_(socket, makecmd("TIME"));
		
		string_vector data;
		bool success = (2 == recv_multi_bulk_reply_(socket, data));
		if (success)
		{
			seconds = boost::lexical_cast<int>(data[0]);
			microseconds = boost::lexical_cast<int>(data[1]);
		}
		return success;
	}
	
    int zadd(const string_type & key, double score, const string_type & member)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zadd" );
			return -1;
		}
		
		int socket = get_socket(key);
		send_(socket, makecmd("ZADD") << key << score << member);
		// Matt: The int reply of ZADD command can be 0 which indicates the key is already existed but the value of it has been updated.
		//recv_int_ok_reply_(socket);
		return recv_int_reply_(socket);
    }

    int zadd(const string_type & key, const string_score_pair & value)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zadd" );
			return -1;
		}
		
		return zadd(key, value.second, value.first);
    }

    // Matt: Make it ok to zrem a non-existed key in a sorted-set
    bool zrem(const string_type & key, const string_type & member)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zrem" );
			return false;
		}
		
		int socket = get_socket(key);
		send_(socket, makecmd("ZREM") << key << member);
		return (recv_int_reply_(socket) == 1);
    }

    double zincrby(const string_type & key, const string_type & member, double increment)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zincrby" );
			return boost::lexical_cast<double>(-1);
		}
		
		int socket = get_socket(key);
		send_(socket, makecmd("ZINCRBY") << key << increment << member);

		std::string retstr = recv_bulk_reply_(socket);
		if (retstr == missing_value())
			return 0.0;
		return boost::lexical_cast<double>(retstr);
    }

    bool zrank(const string_type & key, const string_type & member, int_type &val)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zrank" );
			return false;
		}
		
		int socket = get_socket(key);
		send_(socket, makecmd("ZRANK") << key << member);
		return recv_int_or_nil_reply_(socket, val);
    }

    bool zrevrank(const string_type & key, const string_type & value, int_type &val)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zrevrank" );
			return false;
		}
		
		int socket = get_socket(key);
		send_(socket, makecmd("ZREVRANK") << key << value);
		return recv_int_or_nil_reply_(socket, val);
    }

    void zrange(const string_type & key, int_type start, int_type end, string_vector & out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zrange" );
			return;
		}
		
		int socket = get_socket(key);
		send_( socket, makecmd("ZRANGE") << key << start << end );
		recv_multi_bulk_reply_(socket, out);
    }

  private:
    void convert(const string_vector & in, string_score_vector & out)
    {
      assert( in.size() % 2 == 0 );

      for(size_t i=0; i < in.size(); i += 2)
      {
        const std::string & value = in[i];
        const std::string & str_score = in[i+1];
        double score = boost::lexical_cast<double>(str_score);
        out.push_back( make_pair(value, score) );
      }
    }


  public:

    void zrange(const string_type & key, int_type start, int_type end, string_score_vector & out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zrange" );
			return;
		}
		
      int socket = get_socket(key);
      send_( socket, makecmd("ZRANGE") << key << start << end << "WITHSCORES" );
      string_vector res;
      recv_multi_bulk_reply_(socket, res);
      convert(res, out);
    }

    void zrevrange(const string_type & key, int_type start, int_type end, string_vector & out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zrevrange" );
			return;
		}
		
      int socket = get_socket(key);
      send_( socket, makecmd("ZREVRANGE") << key << start << end );
      recv_multi_bulk_reply_(socket, out);
    }

    void zrevrange(const string_type & key, int_type start, int_type end, string_score_vector & out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zrevrange" );
			return;
		}
		
      int socket = get_socket(key);
      send_( socket, makecmd("ZREVRANGE") << key << start << end << "WITHSCORES" );
      string_vector res;
      recv_multi_bulk_reply_(socket, res);
      convert(res, out);
    }

  protected:
    void zrangebyscore_base(bool withscores, const string_type & key, double min, double max, string_vector & out, int_type offset, int_type max_count, int range_modification)
    {
      int socket = get_socket(key);
      std::string min_str, max_str;
      if( range_modification & exclude_min )
        min_str = "(";
      if( range_modification & exclude_max )
        max_str = "(";

      min_str += boost::lexical_cast<std::string>(min);
      max_str += boost::lexical_cast<std::string>(max);

      makecmd m("ZRANGEBYSCORE");
      m << key << min_str << max_str;

      // Matt: These 2 lines are required to get a reply list with scores. It seems to be overlooked.
      if (withscores)
          m << "WITHSCORES";

      if(max_count != -1 || offset > 0)
      {
        std::cerr << "Adding limit: " << offset << " " << max_count << std::endl;
        m << "LIMIT" << offset << max_count;
      }

      send_(socket, m);
      recv_multi_bulk_reply_(socket, out);
    }

  public:
    void zrangebyscore(const string_type & key, double min, double max, string_vector & out, int_type offset = 0, int_type max_count = -1, int range_modification = 0)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zrangebyscore" );
			return;
		}
		
      zrangebyscore_base(false, key, min, max, out, offset, max_count, range_modification);
    }

    void zrangebyscore(const string_type & key, double min, double max, string_score_vector & out, int_type offset = 0, int_type max_count = -1, int range_modification = 0)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zrangebyscore" );
			return;
		}
		
      string_vector res;
      zrangebyscore_base(true, key, min, max, res, offset, max_count, range_modification);
      convert(res, out);
    }

    int_type zcount(const string_type & key, double min, double max, int range_modification = 0)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zcount" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      std::string min_str, max_str;
      if( range_modification & exclude_min )
        min_str = "(";
      if( range_modification & exclude_max )
        max_str = "(";

      min_str += boost::lexical_cast<std::string>(min);
      max_str += boost::lexical_cast<std::string>(max);

      send_(socket, makecmd("ZCOUNT") << key << min_str << max_str);
      return recv_int_reply_(socket);
    }

    int_type zremrangebyrank( const string_type & key, int_type start, int_type end )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zremrangebyrank" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("ZREMRANGEBYRANK") << key << start << end);
      return recv_int_reply_(socket);
    }

    int_type zremrangebyscore( const string_type& key, double min, double max )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zremrangebyscore" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("ZREMRANGEBYSCORE") << key << min << max);
      return recv_int_reply_(socket);
    }

    int_type zcard( const string_type & key )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zcard" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("ZCARD") << key);
      return recv_int_reply_(socket);
    }

    double zscore( const string_type& key, const string_type& element )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zscore" );
			return boost::lexical_cast<double>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("ZSCORE") << key << element);

	  std::string retstr = recv_bulk_reply_(socket);
	  if (retstr == missing_value())
		  return 0.0;
      return boost::lexical_cast<double>(retstr);
    }

    int_type zunionstore( const string_type & dstkey, const string_vector & keys, const std::vector<double> & weights = std::vector<double>(), aggregate_type aggragate = aggregate_sum )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zunionstore" );
			return boost::lexical_cast<double>(-1);
		}
		
      int dst_socket = get_socket(dstkey);
      int socket = get_socket(keys);
      if(socket != dst_socket)
        throw std::runtime_error("feature is not available in cluster mode");

      makecmd m("ZUNIONSTORE");
      m << dstkey << keys.size() << keys;

      if( weights.size() > 0 )
      {
        assert(keys.size() == weights.size());
        m << "WEIGHTS" << weights;
      }

      m << "AGGREGATE";
      switch(aggragate)
      {
        case aggregate_sum:
          m << "SUM";
          break;
        case aggregate_min:
          m << "MIN";
          break;
        case aggregate_max:
          m << "MAX";
          break;
        default:
          assert(false);
      }

      send_(socket, m);
      return recv_int_reply_(socket);
    }

    int_type zinterstore(const string_type & dstkey, const string_vector & keys, const std::vector<double> & weights = std::vector<double>(), aggregate_type aggragate = aggregate_sum )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "zinterstore" );
			return boost::lexical_cast<double>(-1);
		}
		
      int dst_socket = get_socket(dstkey);
      int socket = get_socket(keys);
      if(socket != dst_socket)
        throw std::runtime_error("feature is not available in cluster mode");

      makecmd m("ZINTERSTORE");
      m << dstkey << keys.size() << keys;
      if( weights.size() > 0 )
      {
        assert(keys.size() == weights.size());
        m << "WEIGHTS" << weights;
      }

      m << "AGGREGATE";
      switch(aggragate)
      {
        case aggregate_sum:
          m << "SUM";
          break;
        case aggregate_min:
          m << "MIN";
          break;
        case aggregate_max:
          m << "MAX";
          break;
        default:
          assert(false);
      }

      send_(socket, m);
      return recv_int_reply_(socket);
    }

    bool hset( const string_type & key, const string_type & field, const string_type & value )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "hset" );
			return false;
		}
      int socket = get_socket(key);
      send_(socket, makecmd("HSET") << key << field << value);
      //return recv_int_reply_(socket) == 1;
      int recvInt = recv_int_reply_(socket);
	  //std::cout<<"recvInt="<<recvInt<<std::endl;
	  return recvInt == 1 || recvInt == 0;
    }

    string_type hget( const string_type & key, const string_type & field )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "hget" );
			return missing_value();
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("HGET") << key << field);
      return recv_bulk_reply_(socket);
    }

    bool hsetnx( const string_type & key, const string_type & field, const string_type & value )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "hsetnx" );
			return false;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("HSETNX") << key << field << value);
      return recv_int_reply_(socket) == 1;
    }

    void hmset( const string_type & key, const string_vector & fields, const string_vector& values )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "hmset" );
			return;
		}
		
      int socket = get_socket(key);
      makecmd m("HMSET");
      m << key;
      assert( fields.size() == values.size() );

      for(size_t i=0; i < fields.size(); i++)
        m << fields[i] << values[i];

      send_(socket, m);
      recv_ok_reply_(socket);
    }

    void hmset( const string_type & key, const string_pair_vector & field_value_pairs )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "hmset" );
			return;
		}
		
      int socket = get_socket(key);
      makecmd m("HMSET");
      m << key;

      for(size_t i=0; i < field_value_pairs.size(); i++)
        m << field_value_pairs[i].first << field_value_pairs[i].second;

      send_(socket, m);
      recv_ok_reply_(socket);
    }

    void hmget( const string_type & key, const string_vector & fields, string_vector & out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "hmget" );
			return;
		}
		
      int socket = get_socket(key);
      makecmd m("HMGET");
      m << key;

      for(size_t i=0; i < fields.size(); i++)
        m << fields[i];

      send_(socket, m);
      recv_multi_bulk_reply_(socket, out);
    }

    int_type hincrby( const string_type & key, const string_type & field, int_type by )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "hincrby" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("HINCRBY") << key << field << by);
      return recv_int_reply_(socket);
    }

    bool hexists( const string_type & key, const string_type & field )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "hexists" );
			return false;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("HEXISTS") << key << field);
      return recv_int_reply_(socket) == 1;
    }

    bool hdel( const string_type& key, const string_type& field )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "hdel" );
			return false;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("HDEL") << key << field);
      return recv_int_reply_(socket) == 1;
    }

    int_type hlen( const string_type & key )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "hlen" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("HLEN") << key);
      return recv_int_reply_(socket);
    }

    void hkeys( const string_type & key, string_vector & out )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "hkeys" );
			return;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("HKEYS") << key);
      recv_multi_bulk_reply_(socket, out);
    }

    void hvals( const string_type & key, string_vector & out )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "hvals" );
			return;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("HVALS") << key);
      recv_multi_bulk_reply_(socket, out);
    }

    void hgetall( const string_type & key, string_pair_vector & out )
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "hgetall" );
			return;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("HGETALL") << key);
      string_vector s;
      recv_multi_bulk_reply_(socket, s);
      for(size_t i = 0; i < s.size(); i+=2)
        out.push_back( make_pair(s[i], s[i+1]) );
    }

    void select(int_type dbindex)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "select" );
			return;
		}
		
      BOOST_FOREACH(const connection_data & con, connections_)
      {
        send_(con.socket, makecmd("SELECT") << dbindex);
      }

      BOOST_FOREACH(connection_data & con, connections_)
      {
        recv_ok_reply_(con.socket);
        con.dbindex = dbindex;
      }
    }

    void select(int_type dbindex, const connection_data & con)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "select" );
			return;
		}
		
      int socket = con.socket;
      send_(socket, makecmd("SELECT") << dbindex);
      recv_ok_reply_(socket);
      BOOST_FOREACH(connection_data & cur_con, connections_)
      {
        if( cur_con == con )
          cur_con.dbindex = dbindex;
      }
    }

    void move(const string_type & key,
                           int_type dbindex)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "move" );
			return;
		}
		
      int socket = get_socket(key);
      send_(socket, makecmd("MOVE") << key << dbindex);
      recv_int_ok_reply_(socket);
    }

    void flushdb()
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "flushdb" );
			return;
		}
		
      BOOST_FOREACH(const connection_data & con, connections_)
      {
        send_(con.socket, makecmd("FLUSHDB"));
      }

      BOOST_FOREACH(const connection_data & con, connections_)
      {
        recv_ok_reply_(con.socket);
      }
    }

    void flushdb(const connection_data & con)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "flushdb" );
			return;
		}
		
      int socket = con.socket;
      send_(socket, makecmd("FLUSHDB"));
      recv_ok_reply_(socket);
    }

    void flushall()
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "flushall" );
			return;
		}
		
      if( connections_.size() > 1 )
        throw std::runtime_error("feature is not available in cluster mode");

      int socket = connections_[0].socket;
      send_(socket, makecmd("FLUSHALL"));
      recv_ok_reply_(socket);
    }

    void flushall(const connection_data & con)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "flushall" );
			return;
		}
		
      int socket = con.socket;
      send_(socket, makecmd("FLUSHALL"));
      recv_ok_reply_(socket);
    }

    int_type sort(const string_type & key,
                  string_vector & out,
                  sort_order order = sort_order_ascending,
                  bool lexicographically = false)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "sort" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      makecmd m("SORT");
      m << key << (order == sort_order_ascending ? "ASC" : "DESC");
      if(lexicographically)
        m << "ALPHA";

      send_(socket, m);
      return recv_multi_bulk_reply_(socket, out);
    }

    int_type sort(const string_type & key,
                  string_vector & out,
                  int_type limit_start,
                  int_type limit_end,
                  sort_order order = sort_order_ascending,
                  bool lexicographically = false)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "sort" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      makecmd m("SORT");
      m << key
        << "LIMIT"
        << limit_start
        << limit_end
        << (order == sort_order_ascending ? "ASC" : "DESC");

      if(lexicographically)
        m << "ALPHA";

      send_(m);
      return recv_multi_bulk_reply_(out);
    }

    int_type sort(const string_type & key,
                  string_vector & out,
                  const string_type & by_pattern,
                  int_type limit_start,
                  int_type limit_end,
                  const string_vector & get_patterns,
                  sort_order order = sort_order_ascending,
                  bool lexicographically = false)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "sort" );
			return boost::lexical_cast<int_type>(-1);
		}
		
      int socket = get_socket(key);
      makecmd m("SORT");

      m << key
      << "BY"    << by_pattern
      << "LIMIT" << limit_start << limit_end;

      string_vector::const_iterator it = get_patterns.begin();
      for ( ; it != get_patterns.end(); ++it)
        m << "GET" << *it;

      m << (order == sort_order_ascending ? "ASC" : "DESC");
      if(lexicographically)
        m << "ALPHA";

      send_(socket, m);
      return recv_multi_bulk_reply_(socket, out);
    }

    void save()
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "save" );
			return;
		}
		
      BOOST_FOREACH(const connection_data & con, connections_)
      {
        send_(con.socket, makecmd("SAVE"));
      }

      BOOST_FOREACH(const connection_data & con, connections_)
      {
        recv_ok_reply_(con.socket);
      }
    }

    void save(const connection_data & con)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "save" );
			return;
		}
		
      send_(con.socket, makecmd("SAVE"));
      recv_ok_reply_(con.socket);
    }

    void bgsave()
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "bgsave" );
			return;
		}
		
      BOOST_FOREACH(const connection_data & con, connections_)
      {
        send_(con.socket, makecmd("BGSAVE"));
      }

      BOOST_FOREACH(const connection_data & con, connections_)
      {
        std::string reply = recv_single_line_reply_(con.socket);
        if(reply != REDIS_STATUS_REPLY_OK && reply != "Background saving started")
          throw protocol_error("Unexpected response on bgsave: '" + reply + "'");
      }
    }

    void bgsave(const connection_data & con)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "bgsave" );
			return;
		}
		
      send_(con.socket, makecmd("BGSAVE"));
      std::string reply = recv_single_line_reply_(con.socket);
      if(reply != REDIS_STATUS_REPLY_OK && reply != "Background saving started")
        throw protocol_error("Unexpected response on bgsave: '" + reply + "'");
    }

    time_t lastsave()
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "lastsave" );
			return boost::lexical_cast<time_t>(-1);
		}
		
      time_t res = 0;
      BOOST_FOREACH(const connection_data & con, connections_)
      {
        int socket = con.socket;
        send_(socket, makecmd("LASTSAVE"));
        time_t cur = recv_int_reply_(socket);
        if(res > 0)
          res = std::min(cur, res);
        else
          res = cur;
      }
      return res;
    }

    time_t lastsave(const connection_data & con)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "lastsave" );
			return boost::lexical_cast<time_t>(-1);
		}
		
      send_(con.socket, makecmd("LASTSAVE"));
      return recv_int_reply_(con.socket);
    }

    void shutdown()
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "shutdown" );
			return;
		}
		
      BOOST_FOREACH(const connection_data & con, connections_)
      {
        int socket = con.socket;
        send_(socket, makecmd("SHUTDOWN"));

        // we expected to get a connection_error as redis closes the connection on shutdown command.

        try
        {
          recv_ok_reply_(socket);
        }
        catch (connection_error & e)
        {
        }
      }
    }

    void shutdown(const connection_data & con)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "shutdown" );
			return;
		}
		
      send_(con.socket, makecmd("SHUTDOWN"));

      // we expected to get a connection_error as redis closes the connection on shutdown command.

      try
      {
        recv_ok_reply_(con.socket);
      }
      catch (connection_error & e)
      {
      }
    }

    void ping()
    {
        ping(connections_[0]);
    }

    void ping(const connection_data & con)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "ping" );
			return;
		}
		
        int socket = con.socket;
        send_(socket, makecmd("PING"));
        try
        {
            std::string reply = recv_single_line_reply_(con.socket);
        }
        catch (connection_error & e)
        {
            throw std::runtime_error("invalid/unknown rely type from redis server");
        }
    }

    void info(const connection_data & con, server_info & out)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "info" );
			return;
		}
		
      int socket = con.socket;
      send_(socket, makecmd("INFO"));
      std::string response = recv_bulk_reply_(socket);

      if (response.empty())
        throw protocol_error("empty");

      string_vector lines;
      split_lines(response, lines);
      if (lines.empty())
        throw protocol_error("empty line for info");

      for(string_vector::const_iterator it = lines.begin(); it != lines.end(); ++it)
      {
        const std::string & line = *it;
        if( line.empty() || line[0] == '#' ) // Don't throw protocol_error on empty and comment lines
          continue;
        string_vector line_parts;
        split(line, ':', line_parts);
        if (line_parts.size() != 2)
          throw protocol_error("unexpected line format for info");

        const std::string & key = line_parts[0];
        const std::string & val = line_parts[1];

        out.param_map[key] = val;

        if (key == "redis_version")
          out.version = val;
        else if (key == "bgsave_in_progress")
          out.bgsave_in_progress = boost::lexical_cast<unsigned long>(val) == 1;
        else if (key == "connected_clients")
          out.connected_clients = boost::lexical_cast<unsigned long>(val);
        else if (key == "connected_slaves")
          out.connected_slaves = boost::lexical_cast<unsigned long>(val);
        else if (key == "used_memory")
          out.used_memory = boost::lexical_cast<unsigned long>(val);
        else if (key == "changes_since_last_save")
          out.changes_since_last_save = boost::lexical_cast<unsigned long>(val);
        else if (key == "last_save_time")
          out.last_save_time = boost::lexical_cast<unsigned long>(val);
        else if (key == "total_connections_received")
          out.total_connections_received = boost::lexical_cast<unsigned long>(val);
        else if (key == "total_commands_processed")
          out.total_commands_processed = boost::lexical_cast<unsigned long>(val);
        else if (key == "uptime_in_seconds")
          out.uptime_in_seconds = boost::lexical_cast<unsigned long>(val);
        else if (key == "uptime_in_days")
          out.uptime_in_days = boost::lexical_cast<unsigned long>(val);
        else if (key == "role")
          out.role = val == "master" ? role_master : role_slave;
        else if (key == "arch_bits")
          out.arch_bits = boost::lexical_cast<unsigned short>(val);
        else if (key == "multiplexing_api")
          out.multiplexing_api = val;
        #ifndef NDEBUG // Ignore new/unknown keys in release mode
          else
            std::cerr << "Found unknown info key '" << key << "'" << std::endl;
          #endif // NDEBUG
      }
    }

    void info(server_info & out)
    {
      info(connections_[0], out);
    }

    int_type publish(const string_type & channel, const string_type & message)
    {
		if( true == subscribe_mode ) {
			errSubscibeMode( "publish" );
			return boost::lexical_cast<int_type>(-1);
		}

      int socket = get_socket(channel);
      send_(socket, makecmd("PUBLISH") << channel << message);
      return recv_int_reply_(socket);
    }

    /* 
     * Subscribe to a named Redis channel. A callback function is required 
     * to forward any received channel messages. This function spawns a new
     * thread listening to channel messages and returns immediately. 
     * Subscribing to multiple channels in a redis client instance is possible.
     * All other Redis commands that irrelevent to sub/pub are disabled.
     *
     * Example:
     *    class MyClass {
     *    public:
     *        void handleChannelMessage(std::string channel, std::string msg) { ... };
     *    };
     *    redis::client client("127.0.0.1", 6379);
     *
     *    client.subscribe("channel_name", boost::bind(&MyClass::handleChannelMessage, this, _1, _2));
     * or with aid of JDelegate.h:
     *    client.subscribe("channel_name", JCALLBACK2(MyClass::handleChannelMessage));
     *    
     */
    void subscribe(const string_type & channel, boost::signal<void (std::string, std::string)>::slot_function_type callback)
    {
      // region Add by adu
      if(unsubscribing)
      {
        if(m_pThread != NULL)
        {
          m_pThread->join();
          delete m_pThread;
          m_pThread = NULL;
        }
      }

      if( NULL == m_pThread ) {
        subscribe_mode = true;
        unsubscribing = false;
        m_pThread = new boost::thread( boost::bind( &redis::base_client<CONSISTENT_HASHER>::workThread, this ) );
      }
		
      {
        boost::unique_lock<boost::shared_mutex> lock(m_channel_mutex);
        m_channel2Signal[channel] = callback;
      }
      // endregion

      command cmd = makecmd("SUBSCRIBE") << key(channel);
      exec_during_subscribing(cmd);
    }

    /*
     * Stop subscribing (if possible).
     */
    void unsubscribe(const string_type & channel)
    {
      // region Add by adu
      command cmd = makecmd("UNSUBSCRIBE") << key(channel);
      exec_during_subscribing(cmd);

      // endregion
    }


    //watch a key
    void watch(const string_type& key) 
    {
      string_vector v;
      v.push_back(key);
      watch(v);
    }

    //watch a list of keys
    void watch(const string_vector& keys)
    {
      if( true == subscribe_mode )
      {
        errSubscibeMode( "watch" );
        return;
      }

      if (keys.empty())
        return;

      makecmd cmd("WATCH");
      for (unsigned int i=0; i<keys.size(); ++i)
      {
        cmd << keys[i];
      }

      int socket = connections_[0].socket;
      send_(socket, cmd);
      recv_ok_reply_(socket);
    }

    //unwatch cancels all watched keys
    void unwatch() 
    {
      if( true == subscribe_mode ) {
        errSubscibeMode( "unwatch" );
        return;
      }

      int socket = connections_[0].socket;
      send_(socket, makecmd("UNWATCH"));
      recv_ok_reply_(socket);
    }

    bool exec_watch_transaction(std::vector<command> & commands, const string_type& key)
    {
      string_vector v;
      v.push_back(key);
      return exec_watch_transaction(commands, v);
    }

    bool exec_watch_transaction(std::vector<command> & commands, const string_vector& keys)
    {
      if( true == subscribe_mode )
      {
        errSubscibeMode( "exec_watch_transaction" );
        return false;
      }

      watch(keys);
      return exec_transaction(commands);
    }

    int expireAt(const string_type & key, unsigned int timestamp)
    {
      if( true == subscribe_mode ) {
        errSubscibeMode( "expireAt" );
        return -1;
      }
		
      int socket = get_socket(key);
      send_(socket, makecmd("EXPIREAT") << key << timestamp);
      return recv_int_reply_(socket);
    }	

	void workThread() {
		int socket = get_socket("SUBSCRIBE:PUBLISH");
		string_vector sv;

		try
		{
			while (!unsubscribing)
			{
				unsigned int msec = 200; // 0.2 sec
#ifdef _WIN32
				struct timeval time;
				time.tv_sec=0;
				time.tv_usec=msec*1000;

				fd_set readfds;
				FD_ZERO(&readfds);
				FD_SET(socket, &readfds);

				int r = ::select(socket+1, NULL, &readfds, NULL, &time);
#else
				struct pollfd pfd;
				pfd.fd = socket;
				pfd.events = POLLIN;
				pfd.revents = 0;

				int r = ::poll(&pfd, 1, msec);
#endif

				if (r > 0) // there is some data to read
				{
					sv.clear();
					int size = recv_multi_bulk_reply_(socket, sv);
					if (size > 0)
					{
						// size[0]: is "message" string, size[1]: channel, size[2]: publish string
						if(sv[0] == "message")
						{
							boost::shared_lock<boost::shared_mutex> lock(m_channel_mutex);
							if( m_channel2Signal.end() != m_channel2Signal.find( sv[size-2] ) )
							{
								boost::signal<void (std::string, std::string)> channalSignal;
								boost::signals::connection signalConnection = channalSignal.connect(m_channel2Signal[sv[size-2]]);
								channalSignal(sv[size-2], sv[size-1]);
								signalConnection.disconnect();
							}
						}
						else if(sv[0] == "unsubscribe")
						{
							boost::unique_lock<boost::shared_mutex> lock(m_channel_mutex);
							m_channel2Signal.erase( sv[1] );

							if( 0 >= m_channel2Signal.size() )
							{
								subscribe_mode = false;
								unsubscribing = true;
							}
						} // end of if(sv[0] == "message")
					} // end of if (size > 0)
				}
				else if (r < 0) // error
				{
					unsubscribing = true;
					break;
				}
			} // end of while()
		} // end of try
		catch (...)
		{
		}
	} // end of workThread

#if 0 // currently unuseable
    struct subscription_t
    {
      string_type channel;
      boost::function<void (const string_type &)> callback;
    };

    void subscribe(const string_type & channel)
    {
      int socket = get_socket(channel);

    }
#endif // 0 // currently unuseable

  private:
    base_client(const base_client &);
    base_client & operator=(const base_client &);

    void backup_cmd_(int socket, const std::string & cmd)
    {
      std::map<int, std::list<std::string> >::iterator it =
          conncmd_queue_.find(socket);
      if (it == conncmd_queue_.end())
      {
        std::list<std::string> q;
        q.push_back(cmd);
        conncmd_queue_.insert(std::make_pair(socket, q));
      }
      else
      {
        it->second.push_back(cmd);
      }
    }

    void pop_all_cmds_(int socket)
    {
      std::map<int, std::list<std::string> >::iterator it =
          conncmd_queue_.find(socket);
      if (it != conncmd_queue_.end())
      {
        if (it->second.empty() == false)
        {
          it->second.clear();
          return;
        }
      }
      throw std::runtime_error("improper conncmd_queue_ pop operation!");
    }

    void pop_cmd_(int socket)
    {
      std::map<int, std::list<std::string> >::iterator it =
          conncmd_queue_.find(socket);
      if (it != conncmd_queue_.end())
      {
        if (it->second.empty() == false)
        {
          it->second.pop_front();
          return;
        }
      }
      throw std::runtime_error("improper conncmd_queue_ pop operation!");
    }

    const std::string& peek_cmd_(int socket)
    {
      std::map<int, std::list<std::string> >::iterator it =
          conncmd_queue_.find(socket);
      if (it != conncmd_queue_.end())
      {
        if (it->second.empty() == false)
        {
		  return it->second.front();
        }
      }
      throw std::runtime_error("improper conncmd_queue_ peek operation!");
    }

    void rotate_cmd_(int socket)
    {
      std::map<int, std::list<std::string> >::iterator it =
          conncmd_queue_.find(socket);
      if (it != conncmd_queue_.end())
      {
        if (it->second.empty() == false)
        {
          std::string cmd = it->second.front();
          it->second.pop_front();
          it->second.push_back(cmd);
          return;
        }
      }
      throw std::runtime_error("improper conncmd_queue_ peek operation!");
    }

    void send_(int socket, const std::string & msg)
    {
#ifndef NDEBUG
      //output_proto_debug(msg, false);
#endif
      int cnt = (conn_wrapper)
        ? conn_wrapper->send(socket, const_cast<char *>(msg.data()), msg.size())
        : anetWrite(socket, const_cast<char *>(msg.data()), msg.size());

      if (cnt == -1)
        throw connection_error(strerror(errno));

      // Matt.20150516: Since request can be redirected, it is required to backup the request content
      //                in case of resending to redirected server.
      backup_cmd_(socket, msg);

      // For now we mark effective_socket as socket since we assume the reply will come from it.
      // If a cluster redirection happens, it changes effective_socket to the redirected socket.
      effective_socket = socket;
    }

    std::string recv_single_line_reply_(int socket)
    {
      std::string line = read_line_cluster(socket);

      #ifndef NDEBUG
      //output_proto_debug(line);
      #endif

      if (line.empty())
        throw protocol_error("empty single line reply");

      if (line.find(REDIS_PREFIX_STATUS_REPLY_ERROR) == 0)
      {
        std::string error_msg = line.substr( strlen(REDIS_PREFIX_STATUS_REPLY_ERROR) );
        if (error_msg.empty())
          error_msg = "unknown error";
        throw protocol_error(error_msg);
      }

      if (line[0] != REDIS_PREFIX_STATUS_REPLY_VALUE)
        throw protocol_error("unexpected prefix for status reply");

      return line.substr(1);
    }

    void recv_ok_reply_(int socket)
    {
      if (recv_single_line_reply_(socket) != REDIS_STATUS_REPLY_OK)
	  {
		  std::cout<< "recv_ok_reply_ no ok" << std::endl;
        throw protocol_error("expected OK response");
	  }
    }
    
    bool subscribe_mode;
    bool unsubscribing;
    boost::signal<void (std::string)> channelmsg_signal;
	std::map< string_type, boost::signal<void (std::string, std::string)>::slot_function_type > m_channel2Signal;
	boost::thread *m_pThread;
	boost::shared_mutex m_channel_mutex;

	int_type recv_bulk_reply_(int socket, char prefix, bool with_multi_bulk = false)
    {
      std::string line = (with_multi_bulk)
          ? read_line(socket)
          : read_line_cluster(socket);

#ifndef NDEBUG
      //output_proto_debug(line);
#endif

      if (line[0] != prefix)
      {
        // If in subscribe mode, allow integer reply show up in a multi-bulk reply.
        // Redis Pub/sub protocol: http://redis.io/topics/pubsub
        if (subscribe_mode && line[0] == ':') 
        {
            int channelid = boost::lexical_cast<int_type>(line.substr(1));
            return channelid | 0xCCCC0000;
        }
#ifndef NDEBUG
        std::cerr << "unexpected prefix for bulk reply (expected '" << prefix << "' but got '" << line[0] << "')" << std::endl;
#endif // NDEBUG
        throw protocol_error("unexpected prefix for bulk reply");
      }

      return boost::lexical_cast<int_type>(line.substr(1));
    }

    std::string recv_bulk_reply_(int socket, bool with_multi_bulk = false)
    {
      int_type length = recv_bulk_reply_(socket, REDIS_PREFIX_SINGLE_BULK_REPLY, with_multi_bulk);

      if (length == -1)
        return missing_value();
      else if (subscribe_mode && ((length & 0xFFFF0000) == 0xCCCC0000))
        return boost::lexical_cast<std::string>(length & 0xFFFF);

      int_type real_length = length + 2;    // CRLF

      std::string data = read_n(effective_socket, real_length);

#ifndef NDEBUG
      //output_proto_debug(data.substr(0, data.length()-2));
#endif

      if (data.empty())
        throw protocol_error("invalid bulk reply data; empty");

      if (data.length() != static_cast<std::string::size_type>(real_length))
        throw protocol_error("invalid bulk reply data; data of unexpected length");

      data.erase(data.size() - 2);

      return data;
    }

    int_type recv_multi_bulk_reply_(int socket, string_vector & out)
    {
      int_type length = recv_bulk_reply_(socket, REDIS_PREFIX_MULTI_BULK_REPLY);

      if (length == -1)
        throw key_error("no such key");

      out.reserve( out.size()+length );

      for (int_type i = 0; i < length; ++i)
        out.push_back(recv_bulk_reply_(effective_socket, true));

      return length;
    }

    int_type recv_multi_bulk_reply_(int socket, string_set & out)
    {
      int_type length = recv_bulk_reply_(socket, REDIS_PREFIX_MULTI_BULK_REPLY);

      if (length == -1)
        throw key_error("no such key");

      for (int_type i = 0; i < length; ++i)
        out.insert(recv_bulk_reply_(effective_socket, true));

      return length;
    }

    template<typename INT_TYPE>
    INT_TYPE recv_int_reply_(int socket)
    {
      std::string line = read_line_cluster(socket);

      #ifndef NDEBUG
      //output_proto_debug(line);
      #endif

      if (line.empty())
        throw protocol_error("invalid integer reply; empty");

      if (line[0] != REDIS_PREFIX_INT_REPLY)
        throw protocol_error("unexpected prefix for integer reply");

      return boost::lexical_cast<INT_TYPE>(line.substr(1));
    }

    int_type recv_int_reply_(int socket)
    {
#if 0
      std::string line = read_line(socket);

#ifndef NDEBUG
      //output_proto_debug(line);
#endif

      if (line.empty())
        throw protocol_error("invalid integer reply; empty");

      if (line[0] != REDIS_PREFIX_INT_REPLY)
        throw protocol_error("unexpected prefix for integer reply");

      return boost::lexical_cast<int_type>(line.substr(1));
#else
      //Matt: utilize recv_int_or_nil_reply_()
      int_type val;

      if (!recv_int_or_nil_reply_(socket, val))
        throw protocol_error("unexpected prefix for integer reply");

      return val;
#endif
    }

    // Matt: for some redis commands which expecting int or nil reply.
    bool recv_int_or_nil_reply_(int socket, int_type &val)
    {
      std::string line = read_line_cluster(socket);

      if (line.empty())
        throw protocol_error("invalid integer reply; empty");

      bool ret = true;
      if (line[0] != REDIS_PREFIX_INT_REPLY)
      {
        // check if it is a null bulk reply.
        if (line.substr(0, 3) == REDIS_NULL_BULK_REPLY)
        {
          ret = false;
        }
        else
        {
          throw protocol_error("unexpected prefix for neither integer nor nil reply");
        }
      }
      else
      {
        val = boost::lexical_cast<int_type>(line.substr(1));
      }

      return ret;
    }

    void recv_int_ok_reply_(int socket)
    {
      if (recv_int_reply_(socket) != 1)
        throw protocol_error("expecting int reply of 1");
    }

    inline int get_socket(const string_type & key)
    {
      size_t con_count = connections_.size();
      if(con_count == 1)
        return connections_[0].socket;
      size_t slot = hasher_( key, static_cast<const std::vector<connection_data> &>(connections_) );
      return connections_[slotmap[slot]].socket;
    }

    int get_socket(const string_vector & keys)
    {
      assert( !keys.empty() );

      if( connections_.size() == 1 )
        return connections_[0].socket;

      int socket = -1;
      for(size_t i=0; i < keys.size(); i++)
      {
        int cur_socket = get_socket(keys[i]);
        if(i > 0 && socket != cur_socket)
          return -1;
          //throw std::runtime_error("not possible in cluster mode");

        socket = cur_socket;
      }

      return socket;
    }

#ifndef NDEBUG
    void output_proto_debug(const std::string & data, bool is_received = true)
    {
      std::string escaped_data(data);
      size_t pos;
      while ((pos = escaped_data.find("\n")) != std::string::npos)
        escaped_data.replace(pos, 1, "\\n");
      while ((pos = escaped_data.find("\r")) != std::string::npos)
        escaped_data.replace(pos, 1, "\\r");

      std::cerr << ::time(NULL) << ": "
                                 << (is_received ? "RECV '" : "SEND '")
                                 << escaped_data
                                 << "'"
                                    << std::endl;
    }
#endif

    std::vector<std::string>::size_type split(const std::string & str, char delim, std::vector<std::string> & elems)
    {
      std::stringstream ss(str);
      std::string item;
      std::vector<std::string>::size_type n = 0;
      while (getline(ss, item, delim))
      {
        elems.push_back(item);
        ++n;
      }
      return n;
    }

    inline std::string & rtrim(std::string & str, const std::string & ws = REDIS_WHITESPACE)
    {
      std::string::size_type pos = str.find_last_not_of(ws);
      str.erase(pos + 1);
      return str;
    }

    inline void split_lines(const std::string & str, std::vector<std::string> & elems)
    {
      split(str, '\n', elems);
      for (std::vector<std::string>::iterator it = elems.begin(); it != elems.end(); ++it)
        rtrim(*it);
    }

    // Determine whether the input redis response is a cluster redirecting error message.
    // Proceed the redirection and memorize the slot-to-server mapping in local.
    // Return the same socket id as 'socket' when no redirecting is needed.
    // Otherwise return the redirected socket id.
    // Throw exception when error happens during redirecting.
    int handle_redirect_errmsg(int socket, const std::string& line)
    {
      if (line.find(REDIS_PREFIX_STATUS_REPLY_REDIRECT) != 0)
        return socket; // definitely not a redirecting error message

      // A valid redirecting error message should be in the form of:
      //   -MOVED 3999 127.0.0.1:6381
      //          ^slot ^host ident string
      int ident_begin_idx = 7; // 7 is the length of "-MOVED ", and the begin index of slot string.
      while ((line[ident_begin_idx] != ' ') && (ident_begin_idx < (int)line.size()))
        ident_begin_idx++;
      if (ident_begin_idx == (int)line.size())
        throw protocol_error("Invalid redis redirecting error message. -- slot index");

      std::string slotstr = line.substr(7, ident_begin_idx - 7);
      unsigned int slotidx = boost::lexical_cast<unsigned int>(slotstr);

      ident_begin_idx++;
      std::string identstr = line.substr(ident_begin_idx);
      if (identstr.find("127.0.0.1:") == 0)
      {
        // If the redirected server host is '127.0.0.1' (multiple redis server 
        // instances running on the same host, which should not be a common case
        // in production environment), the host of current socket should be used
        // instead. (Using 127.0.0.1 will connect to myself...)
        // TODO: What about IPv6 ?
        for (unsigned int i = 0; i < connections_.size(); ++i)
        {
          if (socket == connections_[i].socket)
          {
            identstr = connections_[i].host;
            identstr += ":";
            identstr += line.substr(ident_begin_idx + 10); // 10: length of "127.0.0.1:"
          }
        }
      }

      std::map<std::string, unsigned int>::iterator it =
          connident_map_.find(identstr);
      unsigned int connidx = 0;
      if (it == connident_map_.end())
      {
        size_t colonidx = identstr.find_first_of(':');
        if (colonidx == std::string::npos)
          throw protocol_error("Invalid redis redirecting error message. -- ident string");

        std::string host = identstr.substr(0, colonidx);
        unsigned short port = boost::lexical_cast<unsigned short>(identstr.substr(colonidx+1));

        // setup a new connection to a redirected cluster server node.
        connection_data con;
        con.host = host;
        con.port = port;
        con.dbindex = 0; // TODO: Assuming all cluster node has no password.
        init(con);
        connections_.push_back(con);
        connidx = (unsigned int)(connections_.size() - 1);
        connident_map_.insert(std::make_pair(con.ident, connidx));
      }
      else
      {
        connidx = it->second;
      }
      slotmap[slotidx] = connidx;

      return connections_[connidx].socket;
    }

    // Reads N bytes from given blocking socket.

	ssize_t recv_or_throw(int fd, void* buf, size_t n, int flags)
	{
		ssize_t bytes_received;

		do
		{
			bytes_received = (conn_wrapper)
				? conn_wrapper->recv(fd, (BUFPTR)buf, n, flags)
				: ::recv(fd, (BUFPTR)buf, n, flags);
		}
		while(bytes_received < static_cast<ssize_t>(0) && errno == EINTR);

		if( bytes_received == static_cast<ssize_t>(0) )
			throw connection_error("connection was closed");

		// Handle receive errors. I overlooked it totally, thanks mkx!
		if( bytes_received == static_cast<ssize_t>(-1) )
			throw connection_error(std::string("recv error: ") + strerror(errno));

		return bytes_received;
	}

    std::string read_n(int socket, ssize_t n)
    {
      // changed char* to vector<char> buffer to don't leak memory on exceptions (and also because I hate this delete stuff)
      // C++0x TODO: use a std::string here and it's non-const data() member instead of the vector<char> indirection
      std::vector<char> buffer(n);

      char* buf_start = &buffer[0];
      char* bp = buf_start;
      ssize_t bytes_read = 0;

      while (bytes_read != n)
      {
        ssize_t bytes_received = recv_or_throw(socket, bp, n - (bp - buf_start), 0);

        bytes_read += bytes_received;
        bp         += bytes_received;
      }

      return std::string( buf_start, n );
    }

    reply_t next_reply_type(int socket)
    {
      char reply_prefix[1];
      recv_or_throw(socket, reply_prefix, 1, MSG_PEEK);

      switch( reply_prefix[0] )
      {
        case REDIS_PREFIX_STATUS_REPLY_VALUE:
          return status_code_reply;
        case REDIS_PREFIX_STATUS_REPLY_ERR_C:
          return error_reply;
        case REDIS_PREFIX_INT_REPLY:
          return int_reply;
        case REDIS_PREFIX_SINGLE_BULK_REPLY:
          return bulk_reply;
        case REDIS_PREFIX_MULTI_BULK_REPLY:
          return multi_bulk_reply;
      }

      throw std::runtime_error("invalid/unknown rely type from redis server");
    }

    reply_data_t recv_generic_reply_(int socket)
    {
      reply_data_t res;
      res.first = next_reply_type(socket);
      effective_socket = socket;

      switch( res.first )
      {
        case status_code_reply:
          res.second = read_line_cluster(socket).substr(1);
          break;
		case error_reply:
          {
            std::string line = read_line(socket);
            if (line.find(REDIS_PREFIX_STATUS_REPLY_REDIRECT) == 0)
            {
              res.first = moved_reply;
              res.second = line;
              rotate_cmd_(socket);
            }
            else
            {
              res.second = line.substr(strlen(REDIS_PREFIX_STATUS_REPLY_ERROR));
              pop_cmd_(socket);
            }
          }
          break;
        case int_reply:
          res.second = recv_int_reply_(socket);
          break;
        case bulk_reply:
          res.second = recv_bulk_reply_(socket);
          break;
        case multi_bulk_reply:
        {
          string_vector v;
          recv_multi_bulk_reply_( socket, v );
          res.second = v;
          break;
        }
        case no_reply:
          assert(false);
      }
      return res;
    }

    // Reads a single line of character data from the given blocking socket.
    // Returns the line that was read, not including EOL delimiter(s).  Both LF
    // ('\n') and CRLF ("\r\n") delimiters are supported.  If there was an I/O
    // error reading from the socket, connection_error is raised.  If max_size
    // bytes are read before finding an EOL delimiter, a blank string is
    // returned.

    std::string read_line(int socket, ssize_t max_size = 2048)
    {
      assert(socket > 0);
      assert(max_size > 0);

      //std::ostringstream oss;
      std::string line;
      line.reserve(2048);

      enum { buffer_size = 64 };
      char buffer[buffer_size];
      memset(buffer, 0, buffer_size);

      ssize_t total_bytes_read = 0;
      bool found_delimiter = false;

      while (total_bytes_read < max_size && !found_delimiter)
      {
        // Peek at what's available.

        ssize_t bytes_received = recv_or_throw(socket, buffer, buffer_size, MSG_PEEK);

        // Some data is available; Length might be < buffer_size.
        // Look for newline in whatever was read though.

        char * eol = static_cast<char *>(memchr(buffer, '\n', bytes_received));

        // If found, write data from the buffer to the output string.
        // Else, write the entire buffer and continue reading more data.

        ssize_t to_read = bytes_received;

        if (eol)
        {
          to_read = eol - buffer + 1;
          //oss.write(buffer, to_read);
          line.append(buffer, to_read);
          found_delimiter = true;
        }
        else
          //oss.write(buffer, bytes_received);
          line.append(buffer, bytes_received);

          // Now read from the socket to remove the peeked data from the socket's
          // read buffer.  This will not block since we've peeked already and know
          // there's data waiting.  It might fail if we were interrupted however.

          bytes_received = recv_or_throw(socket, buffer, to_read, 0);
      }

      // Construct final line string. Remove trailing CRLF-based whitespace.
      //std::string line = oss.str();
      return rtrim(line, REDIS_LBR);
    }

    // A read_line version which comply with Redis cluster.
    std::string read_line_cluster(int socket, ssize_t max_size = 2048)
    {
      std::string line = read_line(socket, max_size);

      // When in subscribe mode, the message broadcasted from Redis would never contains
      // redirecting error message. And there it no need to pop cmd from conncmd_queue_.
      if (subscribe_mode)
        return line;

      int rsock = handle_redirect_errmsg(socket, line);
      if (rsock != socket) // redirecting has done and rsock is the socket to the redirected server.
      {
        // Resend the request and get the response back.
        send_(rsock, peek_cmd_(socket));
        line = read_line_cluster(rsock, max_size);
        effective_socket = rsock;
      }
      pop_cmd_(socket);
      return line;
    }

  private:
    std::vector<connection_data> connections_;
    
    // Matt.20150516: T¡³ comply with Redis 3.x cluster protocol
    std::map<int, std::list<std::string> > conncmd_queue_; // Map socket id to its backup command queue.
    unsigned int slotmap[REDIS_CLUSTER_SLOT_NUM]; // Map slot id to index in connections_
    std::map<std::string, unsigned int> connident_map_; // Map connection ident string ({host}:{port}) to index in connections_
    int effective_socket;

    CONSISTENT_HASHER hasher_;
	connection_wrapper *conn_wrapper;
  };

  struct default_hasher
  {
    inline size_t operator()(const std::string & key, const std::vector<connection_data> & connections)
    {
#if 0
      return boost::hash<std::string>()(key) % connections.size();
#endif
      // Matt.20150516: To comply with Redis 3.x cluster slot-hasher
      //                http://redis.io/topics/cluster-spec

      static const unsigned short crc16tab[256] = {
          0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
          0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
          0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
          0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
          0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
          0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
          0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
          0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
          0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
          0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
          0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
          0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
          0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
          0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
          0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
          0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
          0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
          0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
          0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
          0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
          0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
          0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
          0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
          0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
          0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
          0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
          0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
          0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
          0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
          0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
          0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
          0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
      };
      int counter;
      unsigned short crc = 0;
      const char *buf = key.c_str();
      int len = (int)key.size();
      do // hash tag detection
      {
          int s, e; /* start-end indexes of { and } */
          /* Search the first occurrence of '{'. */
          for (s = 0; s < len; s++)
			if (buf[s] == '{') break;
          /* No '{' ? Hash the whole key. This is the base case. */
          if (s == len) break;
          /* '{' found? Check if we have the corresponding '}'. */
          for (e = s + 1; e < len; e++)
            if (buf[e] == '}') break;
          /* No '}' or nothing between {} ? Hash the whole key. */
          if (e == len || e == s + 1) break;
          /* If we are here there is both a { and a } on its right. Hash
           * what is in the middle between { and }. */
		  buf += (s + 1);
		  len = e - s - 1;
	  } while (0);
      for (counter = 0; counter < len; counter++)
          crc = (crc << 8) ^ crc16tab[((crc >> 8) ^ *buf++) & 0x00FF];
      return crc % REDIS_CLUSTER_SLOT_NUM;
    }
  };

  typedef base_client<default_hasher> client;

  class distributed_value
  {
  protected:
    explicit distributed_value(const client::string_type & key, client & client_conn)
     : client_conn_(&client_conn),
       key_(key)
    {
    }

  public:
    virtual ~distributed_value()
    {
    }

    inline const client::string_type & key() const
    {
      return key_;
    }

    bool exists() const
    {
      return client_conn_->exists(key_);
    }

    void del()
    {
      client_conn_->del(key_);
    }

    void rename(const client::string_type & new_name)
    {
      client_conn_->rename(key_, new_name);
      key_ = new_name;
    }

    bool renamenx(const client::string_type & new_name)
    {
      if( client_conn_->renamenx(key_, new_name) )
      {
        key_ = new_name;
        return true;
      }

      return false;
    }

    void expire(unsigned int secs)
    {
      client_conn_->expire(key_, secs);
    }

    int ttl() const
    {
      return client_conn_->ttl(key_);
    }

    void move(client::int_type dbindex)
    {
      client_conn_->move(key_, dbindex);
    }

    client::datatype type() const
    {
      return client_conn_->type(key_);
    }

  protected:
    redis::client* client_conn_;

  private:
    distributed_value & operator=(const distributed_value &)
    {
      return *this;
    }

    client::string_type key_;
  };

  class distributed_string : public distributed_value
  {
  public:
    explicit distributed_string(const client::string_type & key, client & client_conn)
    : distributed_value(key, client_conn)
    {
    }

    distributed_string(const client::string_type & key, const client::string_type & default_value, redis::client & client_conn)
    : distributed_value(key, client_conn)
    {
      setnx(default_value);
    }

    operator client::string_type() const
    {
      return client_conn_->get(key());
    }

    inline client::string_type str() const
    {
      return *this;
    }

    distributed_string & operator=(const client::string_type & value)
    {
      client_conn_->set(key(), value);
      return *this;
    }

    distributed_string & operator=(const distributed_string & other_str)
    {
      if( key() != other_str.key() )
        *this = other_str.str();

      return *this;
    }

    client::string_type getset(const client::string_type & new_value)
    {
      return client_conn_->getset(key(), new_value);
    }

    bool setnx(const client::string_type & value)
    {
      return client_conn_->setnx(key(), value);
    }

    void setex(const client::string_type & value, unsigned int secs)
    {
      client_conn_->setex(key(), value, secs);
    }

    size_t append(const client::string_type & value)
    {
      return client_conn_->append(key(), value);
    }

    distributed_string & operator+=(const client::string_type & value)
    {
      append(value);
      return *this;
    }

    client::string_type substr(int start, int end) const
    {
      return client_conn_->substr(key(), start, end);
    }
  };

  template<typename INT_TYPE>
  class distributed_base_int : public distributed_value
  {
  private:
    BOOST_STATIC_ASSERT( std::numeric_limits<INT_TYPE>::is_integer );

  public:
    typedef INT_TYPE int_type;

    explicit distributed_base_int(const client::string_type & key, client & client_conn)
    : distributed_value(key, client_conn)
    {
    }

    distributed_base_int(const client::string_type & key, int_type default_value, client & client_conn)
    : distributed_value(key, client_conn)
    {
      setnx(default_value);
    }

    distributed_base_int & operator=(int_type val)
    {
      client_conn_->set(key(), boost::lexical_cast<client::string_type>(val));
      return *this;
    }

    distributed_base_int & operator=(const distributed_base_int & other)
    {
      if(key() != other.key())
        client_conn_->set(key(), boost::lexical_cast<client::string_type>(other.to_int()));
      return *this;
    }

    operator int_type() const
    {
      return to_int_type( client_conn_->get(key()) );
    }

    int_type to_int() const
    {
      return *this;
    }

    bool setnx(const int_type & value)
    {
      return client_conn_->setnx(key(), boost::lexical_cast<client::string_type>(value) );
    }

    void setex(const int_type & value, unsigned int secs)
    {
      client_conn_->setex(key(), boost::lexical_cast<client::string_type>(value), secs);
    }

    int_type operator++()
    {
      return client_conn_->incr<int_type>(key());
    }

    int_type operator++(int)
    {
      return client_conn_->incr<int_type>(key()) - 1;
    }

    int_type operator--()
    {
      return client_conn_->decr<int_type>(key());
    }

    int_type operator--(int)
    {
      return client_conn_->decr<int_type>(key()) + 1;
    }

    int_type operator+=(int_type val)
    {
      return client_conn_->incrby<int_type>(key(), val);
    }

    int_type operator-=(int_type val)
    {
      return client_conn_->decrby<int_type>(key(), val);
    }

  private:
    static int_type to_int_type(const client::string_type & val)
    {
      try
      {
        return boost::lexical_cast<int_type>( val );
      }
      catch(boost::bad_lexical_cast & e)
      {
        throw value_error("value is not of integer type");
      }
    }
  };

  typedef distributed_base_int<short>           distributed_short;
  typedef distributed_base_int<ushort>          distributed_ushort;

  typedef distributed_base_int<int>             distributed_int;
  typedef distributed_base_int<uint>            distributed_uint;

  typedef distributed_base_int<long>            distributed_long;
  typedef distributed_base_int<ulong>           distributed_ulong;

  // TODO: lexical_cast treats int8_t/uint8_t as char/uchar
  //typedef distributed_base_int<boost::int8_t>   distributed_int8;
  //typedef distributed_base_int<boost::uint8_t>  distributed_uint8;

  typedef distributed_base_int<boost::int16_t>  distributed_int16;
  typedef distributed_base_int<boost::uint16_t> distributed_uint16;

  typedef distributed_base_int<boost::int32_t>  distributed_int32;
  typedef distributed_base_int<boost::uint32_t> distributed_uint32;

#ifndef BOOST_NO_INT64_T
  typedef distributed_base_int<boost::int64_t>  distributed_longlong;
  typedef distributed_base_int<boost::uint64_t> distributed_ulonglong;

  typedef distributed_base_int<boost::int64_t>  distributed_int64;
  typedef distributed_base_int<boost::uint64_t> distributed_uint64;
#endif // BOOST_NO_INT64_T

#if 0
// not yet working correctly!
  /**
   * This class provides a subset of the functionality that is provided by distributed_int.
   * As it provides only atomic features and is limited to the things that are required to work without
   * a sequence in redis it is harder to missuse the distributed sequence than it is to missuse shared_int.
   */
  template<typename INT_TYPE>
  class distributed_base_sequence
  {
  public:
    distributed_base_sequence(const client::string_type & name, client & con)
    : shr_int_(name, con)
    {
    }

    distributed_base_sequence(const client::string_type & name, INT_TYPE initial_value, client & con)
    : shr_int_(name, initial_value, con)
    {
    }

    /**
     * Gets the next free value from the sequence. If no initial_value is given, this starts with 0.
     */
    INT_TYPE get_next_value()
    {
      return cur_val_ = shr_int_++;
    }

    INT_TYPE get_global_max_value() const
    {
      return shr_int_-1; // -1 because we work with post increment
    }

    inline INT_TYPE get_current_local_value() const
    {
      assert(cur_val_); // You can not call get_current_local_value if you have not called get_next_value
      return *cur_val_;
    }

    /**
     * Lets the sequence skip the given count of values.
     */
    void seek(INT_TYPE by)
    {
      INT_TYPE res = (shr_int_ += by);
      return res-1; // -1 because we work with post increment
    }

  private:
    distributed_base_int<INT_TYPE> shr_int_;
    boost::optional<INT_TYPE> cur_val_;
  };

  typedef distributed_base_sequence<boost::intmax_t> distributed_sequence;

#ifndef TIMEOUT_SEC
#define TIMEOUT_SEC 60
#endif

  /**
   * Supports the Lockable and TimedLockable concepts from Boost.Thread/C++0x.
   */
  class distributed_mutex
  {
  private:
    boost::int32_t tstamp_val( const boost::posix_time::time_duration & in_future = boost::posix_time::time_duration() )
    {
      boost::posix_time::ptime t = boost::posix_time::second_clock::universal_time();
      boost::posix_time::ptime time_t_epoch( boost::gregorian::date(1970, 1, 1) );
      t += in_future;
      boost::posix_time::time_duration timeout_tstamp = t - time_t_epoch;
      return timeout_tstamp.total_seconds();
    }

  public:
    distributed_mutex(const client::string_type & name, client & con)
    : con_(&con), name_(name)
    {
      std::string timeout_str = boost::lexical_cast<std::string>( tstamp_val( boost::posix_time::seconds(TIMEOUT_SEC) ) );

      if( con_->setnx(name_, timeout_str) )
        con_->rpush(name_ + ":list", timeout_str);
    }

    ~distributed_mutex()
    {
    }

    void lock()
    {
      std::string timeout_tstamp_str;
      while(true)
      {
        timeout_tstamp_str = con_->get(name_);
        boost::int32_t timeout_tstamp = boost::lexical_cast<boost::int32_t>(timeout_tstamp_str);
        boost::int32_t diff = tstamp_val( boost::posix_time::seconds(TIMEOUT_SEC) ) - timeout_tstamp;
        if( diff < 1 )
          diff = 1;
        std::string token = con_->blpop(name_ + ":list", diff);
        if( token == client::missing_value() )
        {
          if( timeout_tstamp_str == con_->get(name_) )
          {
            timeout_tstamp_str = boost::lexical_cast<std::string>( tstamp_val( boost::posix_time::seconds(TIMEOUT_SEC) ) );
            con_->set(name_, timeout_tstamp_str);
            con_->rpush(name_ + ":list", timeout_tstamp_str);
          }
          continue;
        }

        timeout_tstamp_str = con_->get(name_);
        if( token == timeout_tstamp_str )
          break;
      }

      std::string new_timeout_tstamp_str = boost::lexical_cast<std::string>( tstamp_val( boost::posix_time::seconds(TIMEOUT_SEC) ) );
      std::string val = con_->getset(name_, timeout_tstamp_str);
      if( timeout_tstamp_str != val )
        lock();

      token_ = new_timeout_tstamp_str;
    }

    void unlock()
    {
      con_->rpush(name_ + ":list", token_);
      token_.clear();
    }

    bool try_lock()
    {
      if( con_->lpop(name_ + ":list") != client::missing_value() )
        return true;

      return false;
    }

    bool timed_lock(boost::system_time const& abs_time)
    {
      boost::posix_time::time_duration dur = abs_time - boost::get_system_time();
      client::int_type timeout = std::max( dur.total_seconds(), 1 );
      std::string res = con_->blpop(name_ + ":list", timeout);
      if( res == client::missing_value() )
        return false;

      return false;
    }

    template<typename DurationType>
    bool timed_lock(DurationType const& rel_time)
    {
      boost::posix_time::time_duration dur( rel_time );
      client::int_type timeout = std::max( dur.total_seconds(), 1 );
      std::string res = con_->blpop(name_ + ":list", timeout);
      if( res == client::missing_value() )
        return false;

      return false;
    }

    typedef boost::unique_lock<distributed_mutex> scoped_timed_lock;
    typedef boost::detail::try_lock_wrapper<distributed_mutex> scoped_try_lock;
    typedef scoped_timed_lock scoped_lock;

  private:
    client* con_;
    client::string_type name_;
    std::string token_;
  };
#endif // 0

  class distributed_list : public distributed_value
  {
  public:
    distributed_list(client & client_conn, const client::string_type & key)
    : distributed_value(key, client_conn)
    {
    }

    void push_back(const client::string_type & value)
    {
      client_conn_->rpush(key(), value);
    }

    void push_front(const client::string_type & value)
    {
      client_conn_->lpush(key(), value);
    }

    client::string_type pop_back()
    {
      return client_conn_->rpop(key());
    }

    client::string_type blocking_pop_back(client::int_type timeout = 0)
    {
      return client_conn_->brpop(key(), timeout);
    }

    client::string_type pop_front()
    {
      return client_conn_->lpop(key());
    }

    client::string_type blocking_pop_front(client::int_type timeout = 0)
    {
      return client_conn_->blpop(key(), timeout);
    }

    size_t size() const
    {
      return client_conn_->llen(key());
    }

    client::string_vector range(client::int_type begin = 0, client::int_type end = -1) const
    {
      client::string_vector res;
      client_conn_->lrange(key(), begin, end, res);
      return res;
    }

    inline client::string_vector to_vector() const
    {
      return range();
    }

    void trim(client::int_type begin, client::int_type end = -1)
    {
      client_conn_->ltrim(key(), begin, end);
    }

    client::string_type operator[](client::int_type index)
    {
      return client_conn_->lindex(key(), index);
    }

    void set(client::int_type index, const client::string_type & value)
    {
      client_conn_->lset(key(), index, value);
    }
  };

  /**
   * This class works on 'redis sets'. For best matching the stl/boost naming conventions it is called
   * distributed_unordered_set and not distributed_set.
   */
  class distributed_unordered_set : public distributed_value
  {
  public:
    distributed_unordered_set(const client::string_type & key, client & client_conn)
     : distributed_value(key, client_conn)
    {
    }

    void insert(const client::string_type & value)
    {
      client_conn_->sadd(key(), value);
    }

    void erase(const client::string_type & value)
    {
      client_conn_->srem(key(), value);
    }

    void clear()
    {
      del();
    }

    client::int_type count() const
    {
      return client_conn_->scard(key());
    }

    client::string_type pop_random()
    {
      return client_conn_->spop(key());
    }

    client::string_type get_random() const
    {
      return client_conn_->srandmember(key());
    }

    bool contains(const client::string_type & value) const
    {
      return client_conn_->sismember(key(), value);
    }
  };

  /**
   * This class works on 'redis sorted sets'. For best matching the stl/boost naming conventions it is called
   * distributed_set.
   */
  class distributed_set : public distributed_value
  {
  public:
    distributed_set(const client::string_type & key, client & client_conn)
    : distributed_value(key, client_conn)
    {
    }
  };
}

inline bool operator==(const redis::distributed_string & sh_str, const redis::client::string_type & str)
{
  return sh_str.str() == str;
}

inline bool operator!=(const redis::distributed_string & sh_str, const redis::client::string_type & str)
{
  return sh_str.str() != str;
}

template <typename ch, typename char_traits>
std::basic_ostream<ch, char_traits>& operator<<(std::basic_ostream<ch, char_traits> & os, const redis::distributed_string & sh_str)
{
  return os << sh_str.str();
}

template <typename ch, typename char_traits>
std::basic_istream<ch, char_traits>& operator>>(std::basic_istream<ch, char_traits> & is, redis::distributed_string & sh_str)
{
  redis::client::string_type s_val;
  is >> s_val;
  sh_str = s_val;
  return is;
}

#endif // REDISCLIENT_H
