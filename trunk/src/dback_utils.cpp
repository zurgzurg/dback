#include <inttypes.h>
#include <cstddef>
#include <string>

#include <boost/thread.hpp>

#include <arpa/inet.h>

#include "dback.h"
#include "btree.h"

namespace dback {

void
ErrorInfo::setErrNum(ErrKind k)
{
    this->errorNum = k;
    this->haveError = true;
}

}
