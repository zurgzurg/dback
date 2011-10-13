#include "config.h"

#include <cstddef>
#include <exception>

#include "dback.h"

namespace dback {

  size_t
  Error::doNothing() const throw()
  {
    return 0;
  }

}
