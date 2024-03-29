#include "config.h"

#include <cstddef>
#include <exception>
#include <string>

#include <inttypes.h>

#include <pthread.h>

#include <cstdarg>
#include <cstring>

#include <list>
#include <vector>
#include <limits>
#include <sstream>
#include <exception>
#include <iostream>
#include <new>

#include <boost/thread.hpp>

#include "dback.h"
#include "btree.h"
#include "r2btree.h"

using namespace std;

using namespace dback;

/****************************************************/
class TestFailure : public exception {
    string msg;

    // attempt to prevent two exceptions from being thrown at the 'same' time
    // TestFailure should only be created if it is going to be thrown, so bump
    // the counter on creation and decrment when it is handled
    static int numActive;

public:
    TestFailure(const char *file, int line, size_t numAsserts);
    ~TestFailure() throw() {;};

    const char *what() const throw();

    static void setUnwindDone() throw();
    static int getNumActive() throw();
};

int TestFailure::numActive = 0;

TestFailure::TestFailure(const char *file, int line, size_t assertNum)
{
    stringstream tmp;
    tmp << "TestFailure excecption: ";
    tmp << file;
    tmp << ":";
    tmp << line;
    if (assertNum > 0) {
	tmp << " assert number ";
	tmp << assertNum;
    }
    tmp << "\n";
    this->msg = tmp.str();
    TestFailure::numActive++;
}

const char *
TestFailure::what() const throw()
{
    const char *r;

    r = this->msg.c_str();

    return r;
}

// static member func
void
TestFailure::setUnwindDone() throw()
{
    TestFailure::numActive--;
}

// static member func
int
TestFailure::getNumActive() throw()
{
    return TestFailure::numActive;
}

/****************************************************/
class TestOption {
public:
    bool verbose;

    TestOption() : verbose(false) {;};
};

/****************************************************/
class TestResult {
public:
    size_t n_run;
    size_t n_pass;
    size_t n_fail;
    size_t n_exceptions;
    size_t n_asserts;

    list<string> tests_e;
    list<string> tests_f;

    TestResult() : n_run(0),
		   n_pass(0),
		   n_fail(0),
		   n_exceptions(0),
		   n_asserts(0) {;};

    void report();
};

void
TestResult::report()
{
    cout << "num_run=" << this->n_run
	 << " num_pass=" << this->n_pass
	 << " num_fail=" << this->n_fail
	 << " num_exceptions=" << this->n_exceptions
	 << " num_asserts=" << this->n_asserts
	 << "\n";
    if (!this->tests_e.empty()) {
	list<string>::iterator iter = this->tests_e.begin();
	cout << "Tests failing with exceptions\n";
	while (iter != this->tests_e.end()) {
	    cout << "    " << *iter << "\n";
	    iter++;
	}
    }

    if (!this->tests_f.empty()) {
	list<string>::iterator iter = this->tests_f.begin();
	cout << "Failing tests\n";
	while (iter != this->tests_f.end()) {
	    cout << "    " << *iter << "\n";
	    iter++;
	}
    }

    return;
}

/****************************************************/
struct TestCase {
    TestResult *result;
    string name;
    bool isPass;
    bool statusSet;
    size_t m_refCount;

    virtual void setup();
    virtual void run();
    virtual void teardown();

    TestCase(const char *nm)
	: result(NULL),
	  name(nm),
	  isPass(false),
	  statusSet(false),
	  m_refCount(1)
    {;};

    void assertTrue(bool, const char *file, int line);
    void setStatus(bool);
    TestCase *makeReference();
    void releaseReference();
};

#define ASSERT_TRUE(c) this->assertTrue(c, __FILE__, __LINE__)

void
TestCase::assertTrue(bool c, const char *fname, int line)
{
    if (this->result)
	this->result->n_asserts++;
    if (c == true)
	return;
    if (TestFailure::getNumActive() > 0)
	return;

    size_t n_asserts = 0;
    if (this->result)
	n_asserts = this->result->n_asserts;
    throw TestFailure(fname, line, n_asserts);
}

void
TestCase::setStatus(bool s)
{
    this->statusSet = true;
    this->isPass = s;
    if (this->result) {
	if (s == true)
	    this->result->n_pass++;
	else
	    this->result->n_fail++;
    }
    return;
}

TestCase *
TestCase::makeReference()
{
    this->m_refCount++;
#if 0
    cout << "make ref for "
	 << this->name
	 << "count="
	 << this->m_refCount
	 << "\n";
#endif
    return this;
}

void
TestCase::releaseReference()
{
    this->m_refCount--;
#if 0
    cout << "release ref for "
	 << this->name
	 << "count="
	 << this->m_refCount
	 << "\n";
#endif
    if (this->m_refCount == 0)
	delete this;
}

void
TestCase::setup()
{
    ;
}

void
TestCase::run()
{
    ;
}

void
TestCase::teardown()
{
    ;
}

/****************************************************/
struct TestSuite {
    list<TestCase *> tests;

    ~TestSuite();

    void run(TestResult *, TestOption *);
    void addTestCase(TestCase *);
};

TestSuite::~TestSuite()
{
    list<TestCase *>::iterator iter;
    iter = this->tests.begin();
    while (iter != this->tests.end()) {
	TestCase *tc = *iter;
	tc->releaseReference();
	iter++;
    }
}

/************/

void
TestSuite::run(TestResult *result, TestOption *opt)
{
    list<TestCase *>::iterator iter;
  
    iter = this->tests.begin();
    while (iter != this->tests.end()) {
	TestCase *tc = *iter;

	if (opt->verbose)
	    cout << "Starting test " << tc->name << "\n";

	tc->result = result;
	try {
	    tc->setup();

	    result->n_run++;
	    tc->statusSet = false;
	    tc->run();

	    if (tc->statusSet == false) {
		result->n_fail++;
		result->tests_f.push_back( tc->name );
	    }

	}
	catch (TestFailure e) {
	    TestFailure::setUnwindDone();
	    if (opt->verbose)
		cout << "    caught test fail exception\n";
	    result->tests_e.push_back( tc->name + string(" ") + e.what() );
	    result->n_exceptions++;
	}
	if (opt->verbose)
	    cout << "    done\n";

	iter++;
    }

    return;
}

void
TestSuite::addTestCase(TestCase *tc)
{
    this->tests.push_back(tc);
    return;
}

/****************************************************/
/****************************************************/
/* basic tests                                      */
/****************************************************/
/****************************************************/
struct TC_Basic01 : public TestCase {
    TC_Basic01() : TestCase("TC_Basic01") {;};
    void run();
};

void
TC_Basic01::run()
{
    ASSERT_TRUE(true);
    this->setStatus(true);
}

/****************************************************/
/****************************************************/
/* basic serization tests                           */
/****************************************************/
/****************************************************/
struct TC_Serial01 : public TestCase {
    TC_Serial01() : TestCase("TC_Serial01") {;};
    void run();
};

void
TC_Serial01::run()
{
    uint8_t buf[10];
    SerialBuffer(&buf[0], sizeof(buf));
    this->setStatus(true);
}

/************/

struct TC_Serial02 : public TestCase {
    TC_Serial02() : TestCase("TC_Serial02") {;};
    void run();
};

void
TC_Serial02::run()
{
    uint8_t buf[1];
    bool s;
    int8_t v;

    SerialBuffer sb(&buf[0], sizeof(buf));
    s = sb.putInt8(0);
    ASSERT_TRUE(s);
    v = 1;
    s = sb.getInt8(&v);
    ASSERT_TRUE(s);
    ASSERT_TRUE(v == 0);

    this->setStatus(true);
}

/************/

struct TC_Serial03 : public TestCase {
    TC_Serial03() : TestCase("TC_Serial03") {;};
    void run();
};

void
TC_Serial03::run()
{
    {
	uint8_t buf[5];
	bool s;
	int8_t i, v;
	
	SerialBuffer sb(&buf[0], sizeof(buf));
	
	s = sb.putInt8(127);
	ASSERT_TRUE(s);
	s = sb.putInt8(1);
	ASSERT_TRUE(s);
	s = sb.putInt8(0);
	ASSERT_TRUE(s);
	s = sb.putInt8(-1);
	ASSERT_TRUE(s);
	s = sb.putInt8(-128);
	ASSERT_TRUE(s);

	v = 1;

	s = sb.getInt8(&v);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == 127);
	s = sb.getInt8(&v);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == 1);
	s = sb.getInt8(&v);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == 0);
	s = sb.getInt8(&v);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == -1);
	s = sb.getInt8(&v);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == -128);

	s = sb.getInt8(&v);
	ASSERT_TRUE(s == false);
    }

    {
	uint8_t buf[3];
	bool s;
	uint8_t i, v;
	
	SerialBuffer sb(&buf[0], sizeof(buf));
	
	s = sb.putUInt8(0);
	ASSERT_TRUE(s);
	s = sb.putUInt8(1);
	ASSERT_TRUE(s);
	s = sb.putUInt8(255);
	ASSERT_TRUE(s);

	s = sb.putUInt8(77);
	ASSERT_TRUE(s == false);

	v = 1;

	s = sb.getUInt8(&v);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == 0);
	s = sb.getUInt8(&v);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == 1);
	s = sb.getUInt8(&v);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == 255);

	s = sb.getUInt8(&v);
	ASSERT_TRUE(s == false);

	s = sb.getUInt8(&v, 0);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == 0);

	s = sb.getUInt8(&v, 1);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == 1);
	s = sb.getUInt8(&v, 2);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == 255);

	s = sb.getUInt8(&v, 3);
	ASSERT_TRUE(s == false);
    }

    {
	uint8_t buf[3];
	bool s;
	uint8_t i, v;
	
	SerialBuffer sb(&buf[0], sizeof(buf));
	
	s = sb.putUInt8(0, 0);
	ASSERT_TRUE(s);
	s = sb.putUInt8(1, 1);
	ASSERT_TRUE(s);
	s = sb.putUInt8(255, 2);
	ASSERT_TRUE(s);

	s = sb.putUInt8(77, 4);
	ASSERT_TRUE(s == false);

	v = 1;
	s = sb.getUInt8(&v, 0);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == 0);
	s = sb.getUInt8(&v, 1);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == 1);
	s = sb.getUInt8(&v, 2);
	ASSERT_TRUE(s);
	ASSERT_TRUE(v == 255);

    }

    this->setStatus(true);
}

/************/

struct TC_Serial04 : public TestCase {
    TC_Serial04() : TestCase("TC_Serial04") {;};
    void run();
};

void
TC_Serial04::run()
{
    uint8_t buf[1];
    bool s;

    SerialBuffer sb(&buf[0], sizeof(buf));

    s = sb.putInt8(0);
    ASSERT_TRUE(s == true);
    s = sb.putInt8(1);
    ASSERT_TRUE(s == false);

    this->setStatus(true);
}

/************/

struct TC_Serial05 : public TestCase {
    TC_Serial05() : TestCase("TC_Serial05") {;};
    void run();
};

void
TC_Serial05::run()
{
    uint8_t buf[1];
    bool s;
    int8_t v;

    SerialBuffer sb(&buf[0], sizeof(buf));
    s = sb.putInt8(22, 0);
    ASSERT_TRUE(s);
    v = 1;
    s = sb.getInt8(&v, 0);
    ASSERT_TRUE(s);
    ASSERT_TRUE(v == 22);

    s = sb.getInt8(&v, 1);
    ASSERT_TRUE(s == false);


    this->setStatus(true);
}

/************/

struct TC_Serial06 : public TestCase {
    TC_Serial06() : TestCase("TC_Serial06") {;};
    void run();
};

void
TC_Serial06::run()
{
    uint8_t buf[1];
    bool s;

    SerialBuffer sb(&buf[0], sizeof(buf));

    s = sb.putInt8(0, 0);
    ASSERT_TRUE(s == true);
    s = sb.putInt8(1, 1);
    ASSERT_TRUE(s == false);

    this->setStatus(true);
}

/************/

struct TC_Serial07 : public TestCase {
    TC_Serial07() : TestCase("TC_Serial07") {;};
    void run();
};

void
TC_Serial07::run()
{
    uint8_t buf[2 * sizeof(int16_t)];
    bool s;
    int16_t v;

    SerialBuffer sb(&buf[0], sizeof(buf));

    v = -1;
    s = sb.putInt16(v);
    ASSERT_TRUE(s == true);

    v = 0;
    s = sb.putInt16(v);
    ASSERT_TRUE(s == true);

    s = sb.putInt16(v);
    ASSERT_TRUE(s == false);

    s = sb.getInt16(&v);
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == -1);

    s = sb.getInt16(&v);
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == 0);

    s = sb.getInt16(&v);
    ASSERT_TRUE(s == false);

    this->setStatus(true);
}

/************/

struct TC_Serial08 : public TestCase {
    TC_Serial08() : TestCase("TC_Serial08") {;};
    void run();
};

void
TC_Serial08::run()
{
    uint8_t buf[2 * sizeof(int16_t)];
    bool s;
    int16_t v;

    SerialBuffer sb(&buf[0], sizeof(buf));

    v = -1;
    s = sb.putInt16(v, 1 * sizeof(v));
    ASSERT_TRUE(s == true);

    v = 0;
    s = sb.putInt16(v, 0 * sizeof(v));
    ASSERT_TRUE(s == true);

    s = sb.putInt16(v, 2 * sizeof(v));
    ASSERT_TRUE(s == false);

    s = sb.getInt16(&v, 1 * sizeof(v));
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == -1);

    s = sb.getInt16(&v, 0 * sizeof(v));
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == 0);

    s = sb.getInt16(&v, 2 * sizeof(v));
    ASSERT_TRUE(s == false);

    this->setStatus(true);
}

/************/

struct TC_Serial09 : public TestCase {
    TC_Serial09() : TestCase("TC_Serial09") {;};
    void run();
};

void
TC_Serial09::run()
{
    uint8_t buf[2 * sizeof(int32_t)];
    bool s;
    int32_t v;

    SerialBuffer sb(&buf[0], sizeof(buf));

    v = -1;
    s = sb.putInt32(v, 1 * sizeof(v));
    ASSERT_TRUE(s == true);

    v = 0;
    s = sb.putInt32(v, 0 * sizeof(v));
    ASSERT_TRUE(s == true);

    s = sb.putInt32(v, 2 * sizeof(v));
    ASSERT_TRUE(s == false);

    s = sb.getInt32(&v, 1 * sizeof(v));
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == -1);

    s = sb.getInt32(&v, 0 * sizeof(v));
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == 0);

    s = sb.getInt32(&v, 2 * sizeof(v));
    ASSERT_TRUE(s == false);

    this->setStatus(true);
}

/************/

struct TC_Serial10 : public TestCase {
    TC_Serial10() : TestCase("TC_Serial10") {;};
    void run();
};

void
TC_Serial10::run()
{
    uint8_t buf[2 * sizeof(int32_t)];
    bool s;
    int32_t v;

    SerialBuffer sb(&buf[0], sizeof(buf));

    v = -1;
    s = sb.putInt32(v);
    ASSERT_TRUE(s == true);

    v = 0;
    s = sb.putInt32(v);
    ASSERT_TRUE(s == true);

    s = sb.putInt32(v);
    ASSERT_TRUE(s == false);

    s = sb.getInt32(&v);
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == -1);

    s = sb.getInt32(&v);
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == 0);

    s = sb.getInt32(&v);
    ASSERT_TRUE(s == false);

    this->setStatus(true);
}


/************/

struct TC_Serial11 : public TestCase {
    TC_Serial11() : TestCase("TC_Serial11") {;};
    void run();
};

void
TC_Serial11::run()
{
    uint8_t buf[2 * sizeof(int16_t)];
    bool s;
    uint16_t v;

    SerialBuffer sb(&buf[0], sizeof(buf));

    v = 0xFFFF;
    s = sb.putUInt16(v);
    ASSERT_TRUE(s == true);

    v = 0;
    s = sb.putUInt16(v);
    ASSERT_TRUE(s == true);

    s = sb.putUInt16(v);
    ASSERT_TRUE(s == false);

    s = sb.getUInt16(&v);
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == 0xFFFF);

    s = sb.getUInt16(&v);
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == 0);

    s = sb.getUInt16(&v);
    ASSERT_TRUE(s == false);

    this->setStatus(true);
}

/************/

struct TC_Serial12 : public TestCase {
    TC_Serial12() : TestCase("TC_Serial12") {;};
    void run();
};

void
TC_Serial12::run()
{
    uint8_t buf[2 * sizeof(int16_t)];
    bool s;
    uint16_t v;

    SerialBuffer sb(&buf[0], sizeof(buf));

    v = 0xFFFF;
    s = sb.putUInt16(v, 1 * sizeof(v));
    ASSERT_TRUE(s == true);

    v = 0;
    s = sb.putUInt16(v, 0 * sizeof(v));
    ASSERT_TRUE(s == true);

    s = sb.putUInt16(v, 2 * sizeof(v));
    ASSERT_TRUE(s == false);

    s = sb.getUInt16(&v, 1 * sizeof(v));
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == 0xFFFF);

    s = sb.getUInt16(&v, 0 * sizeof(v));
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == 0);

    s = sb.getUInt16(&v, 2 * sizeof(v));
    ASSERT_TRUE(s == false);

    this->setStatus(true);
}

/************/

struct TC_Serial13 : public TestCase {
    TC_Serial13() : TestCase("TC_Serial13") {;};
    void run();
};

void
TC_Serial13::run()
{
    uint8_t buf[2 * sizeof(int32_t)];
    bool s;
    uint32_t v;

    SerialBuffer sb(&buf[0], sizeof(buf));

    v = 0xFFFF;
    s = sb.putUInt32(v, 1 * sizeof(v));
    ASSERT_TRUE(s == true);

    v = 0;
    s = sb.putUInt32(v, 0 * sizeof(v));
    ASSERT_TRUE(s == true);

    s = sb.putUInt32(v, 2 * sizeof(v));
    ASSERT_TRUE(s == false);

    s = sb.getUInt32(&v, 1 * sizeof(v));
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == 0xFFFF);

    s = sb.getUInt32(&v, 0 * sizeof(v));
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == 0);

    s = sb.getUInt32(&v, 2 * sizeof(v));
    ASSERT_TRUE(s == false);

    this->setStatus(true);
}

/************/

struct TC_Serial14 : public TestCase {
    TC_Serial14() : TestCase("TC_Serial14") {;};
    void run();
};

void
TC_Serial14::run()
{
    uint8_t buf[2 * sizeof(int32_t)];
    bool s;
    uint32_t v;

    SerialBuffer sb(&buf[0], sizeof(buf));

    v = 0xFFFF;
    s = sb.putUInt32(v);
    ASSERT_TRUE(s == true);

    v = 0;
    s = sb.putUInt32(v);
    ASSERT_TRUE(s == true);

    s = sb.putUInt32(v);
    ASSERT_TRUE(s == false);

    s = sb.getUInt32(&v);
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == 0xFFFF);

    s = sb.getUInt32(&v);
    ASSERT_TRUE(s == true);
    ASSERT_TRUE(v == 0);

    s = sb.getUInt32(&v);
    ASSERT_TRUE(s == false);

    this->setStatus(true);
}

/************/

struct TC_Serial15 : public TestCase {
    TC_Serial15() : TestCase("TC_Serial15") {;};
    void run();
};

void
TC_Serial15::run()
{
    {
	uint8_t buf[2 * sizeof(int32_t)];
	bool s;
	uint32_t v32;
	uint8_t v8;

	SerialBuffer sb(&buf[0], sizeof(buf));

	v32 = 0x04030201;
	s = sb.putUInt32(v32);
	ASSERT_TRUE(s == true);

	s = sb.getUInt8(&v8, 0);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 0x01);

	s = sb.getUInt8(&v8, 1);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 0x02);

	s = sb.getUInt8(&v8, 2);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 0x03);

	s = sb.getUInt8(&v8, 3);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 0x04);
    }

    {
	uint8_t buf[2 * sizeof(int32_t)];
	bool s;
	int32_t v32;
	int8_t v8;

	SerialBuffer sb(&buf[0], sizeof(buf));

	v32 = 0x04030201;
	s = sb.putInt32(v32);
	ASSERT_TRUE(s == true);

	s = sb.getInt8(&v8, 0);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 0x01);

	s = sb.getInt8(&v8, 1);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 0x02);

	s = sb.getInt8(&v8, 2);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 0x03);

	s = sb.getInt8(&v8, 3);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 0x04);
    }

    {
	uint8_t buf[1 * sizeof(int32_t)];
	bool s;
	uint32_t v32;
	int8_t v8;

	SerialBuffer sb(&buf[0], sizeof(buf));

	v32 = 0x7fFDFEFF;
	s = sb.putUInt32(v32);
	ASSERT_TRUE(s == true);

	s = sb.getInt8(&v8, 0);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == -1);

	s = sb.getInt8(&v8, 1);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == -2);

	s = sb.getInt8(&v8, 2);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == -3);

	s = sb.getInt8(&v8, 3);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 127);
    }

    {
	uint8_t buf[1 * sizeof(int32_t)];
	bool s;
	int32_t v32;
	int8_t v8;

	SerialBuffer sb(&buf[0], sizeof(buf));

	v32 = 0x7fFDFEFF;
	s = sb.putInt32(v32);
	ASSERT_TRUE(s == true);

	s = sb.getInt8(&v8, 0);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == -1);

	s = sb.getInt8(&v8, 1);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == -2);

	s = sb.getInt8(&v8, 2);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == -3);

	s = sb.getInt8(&v8, 3);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 127);
    }

    {
	uint8_t buf[1 * sizeof(int16_t)];
	bool s;
	uint16_t v16;
	int8_t v8;

	SerialBuffer sb(&buf[0], sizeof(buf));

	v16 = 0x7fFF;
	s = sb.putUInt16(v16);
	ASSERT_TRUE(s == true);

	s = sb.getInt8(&v8, 0);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == -1);

	s = sb.getInt8(&v8, 1);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 127);
    }

    {
	uint8_t buf[2 * sizeof(int16_t)];
	bool s;
	uint16_t v16;
	uint8_t v8;

	SerialBuffer sb(&buf[0], sizeof(buf));

	v16 = 0x0201;
	s = sb.putUInt16(v16);
	ASSERT_TRUE(s == true);

	s = sb.getUInt8(&v8, 0);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 0x01);

	s = sb.getUInt8(&v8, 1);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 0x02);
    }

    {
	uint8_t buf[2 * sizeof(int16_t)];
	bool s;
	int16_t v16;
	int8_t v8;

	SerialBuffer sb(&buf[0], sizeof(buf));

	v16 = 0x0201;
	s = sb.putInt16(v16);
	ASSERT_TRUE(s == true);

	s = sb.getInt8(&v8, 0);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 0x01);

	s = sb.getInt8(&v8, 1);
	ASSERT_TRUE(s == true);
	ASSERT_TRUE(v8 == 0x02);
    }

    this->setStatus(true);
}


/****************************************************/
/****************************************************/
/* basic btree tests                                */
/****************************************************/
/****************************************************/
namespace dback {

struct TC_BTree00 : public TestCase {
    TC_BTree00() : TestCase("TC_BTree00") {;};
    void run();
};

void
TC_BTree00::run()
{
    BTree b;
    IndexHeader ih;
    PageHeader ph;
    PageAccess pa;
    UUIDKey k;
    ErrorInfo err;
    bool ok;

    ih.nKeyBytes = 16;
    ih.pageSizeInBytes = 4096;
    ih.maxNumNLeafKeys = 0;
    ih.minNumNLeafKeys = 0;
    ih.maxNumLeafKeys = 0;

    ph.parentPageNum = 0;
    ph.numKeys = 0;
    ph.isLeaf = 1;
    ph.pad0 = 0;
    ph.pad1 = 0;

    pa.header = &ph;
    pa.keys = NULL;
    pa.childPtrs = NULL;
    pa.values = NULL;

    b.header = &ih;
    b.root = &pa;
    b.ki = &k;

    uint8_t *key = NULL;
    uint64_t val = 0;

    boost::shared_mutex m;

    ok = b.blockInsertInLeaf(&m, &pa, key, val, &err);
    ASSERT_TRUE(ok == false);
    
    ph.isLeaf = 0;
    ok = b.blockInsertInLeaf(&m, &pa, key, val, &err);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree01 : public TestCase {
    TC_BTree01() : TestCase("TC_BTree01") {;};
    void run();
};

void
TC_BTree01::run()
{
    {
	IndexHeader ih;

	UUIDKey::initIndexHeader(&ih, 56);
	ASSERT_TRUE(ih.nKeyBytes == 16);
	ASSERT_TRUE(ih.pageSizeInBytes == 56);
	ASSERT_TRUE(ih.maxNumNLeafKeys == 2);
	ASSERT_TRUE(ih.minNumNLeafKeys == 1);
	ASSERT_TRUE(ih.maxNumLeafKeys == 2);
    }

    {
	IndexHeader ih;

	UUIDKey::initIndexHeader(&ih, 80);
	ASSERT_TRUE(ih.nKeyBytes == 16);
	ASSERT_TRUE(ih.pageSizeInBytes == 80);
	ASSERT_TRUE(ih.maxNumNLeafKeys == 2);
	ASSERT_TRUE(ih.minNumNLeafKeys == 1);
	ASSERT_TRUE(ih.maxNumLeafKeys == 3);
    }

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree02 : public TestCase {
    TC_BTree02() : TestCase("TC_BTree02") {;};
    void run();
};

void
TC_BTree02::run()
{
    uint8_t buf[4096];
    BTree b;
    PageAccess pa;
    IndexHeader ih;
    uint32_t idx;
    bool found;
    UUIDKey k;

    UUIDKey::initIndexHeader(&ih, sizeof(buf));

    uint8_t key[ ih.nKeyBytes ];

    memset(&buf[0], 0, sizeof(buf));
    pa.header = reinterpret_cast<PageHeader *>(&buf[0]);
    pa.header->isLeaf = 1;
    pa.keys = NULL;
    pa.childPtrs = NULL;
    pa.values = NULL;

    b.header = &ih;
    b.root = &pa;
    b.ki = &k;

    for (uint32_t i = 0; i < ih.nKeyBytes; i++)
	key[i] = 0;

    idx = 2;

    found = b.findKeyPosition(&pa, &key[0], &idx);
    ASSERT_TRUE(found == false);
    ASSERT_TRUE(idx == 0);

    this->setStatus(true);
}

}

/************/

namespace dback {

/**
 * Simple test class for 1 byte keys.
 */
class ShortKey : public KeyInterface {
public:
    int compare(const uint8_t *a, const uint8_t *b);
    void initIndexHeader(IndexHeader *ih, uint32_t pageSize);
};

int
ShortKey::compare(const uint8_t *a, const uint8_t *b)
{
    if (*a < *b)
	return -1;
    else if (*a > *b)
	return 1;
    return 0;
}

void
ShortKey::initIndexHeader(IndexHeader *ih, uint32_t pageSize)
{
    size_t hdr_size = sizeof(PageHeader);
    size_t ptr_size = sizeof(uint32_t);
    size_t data_size = sizeof(uint64_t);
    size_t key_size = 1;

    ih->nKeyBytes = key_size;
    ih->pageSizeInBytes = pageSize;
                          // (pgsize - 8) / 5
    ih->maxNumNLeafKeys = (pageSize - hdr_size) / (key_size + ptr_size);

    ih->minNumNLeafKeys = ih->maxNumNLeafKeys / 2;

                          // (pgsize - 8) / 9
    ih->maxNumLeafKeys = (pageSize - hdr_size) / (key_size + data_size);

    return;
}

struct TC_BTree03 : public TestCase {
    TC_BTree03() : TestCase("TC_BTree03") {;};
    void run();
};

void
TC_BTree03::run()
{
    ShortKey k;
    const size_t bufsize = 28;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys > 1);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initLeafPage(&buf[0]);

    PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    uint8_t a_key[ih.nKeyBytes];
    memset(&a_key[0], 99, sizeof(a_key));

    ErrorInfo err;
    boost::shared_mutex l;
    uint64_t val = 97;
    bool ok;

    ok = b.blockInsertInLeaf(&l, &pa, &a_key[0], val, &err);
    ASSERT_TRUE(ok == true);

    uint32_t idx = 5;
    ok = b.findKeyPosition(&pa, &a_key[0], &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 0);
    ASSERT_TRUE(pa.values[idx] == 97);
    ASSERT_TRUE(pa.keys[0] == 99);

    ok = b.blockInsertInLeaf(&l, &pa, &a_key[0], val, &err);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree04 : public TestCase {
    TC_BTree04() : TestCase("TC_BTree04") {;};
    void run();
};

void
TC_BTree04::run()
{
    ShortKey k;
    const size_t bufsize = 28;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initLeafPage(&buf[0]);

    PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.nKeyBytes == 1);

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uint64_t val;
    bool ok;
    uint32_t idx;

    val = 1;
    a_key = 1;
    ok = b.blockInsertInLeaf(&l, &pa, &a_key, val, &err);
    ASSERT_TRUE(ok == true);

    val = 2;
    a_key = 2;
    ok = b.blockInsertInLeaf(&l, &pa, &a_key, val, &err);
    ASSERT_TRUE(ok == true);

    a_key = 1;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 0);
    ASSERT_TRUE(pa.values[idx] == 1);
 
    a_key = 2;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 1);
    ASSERT_TRUE(pa.values[idx] == 2);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree05 : public TestCase {
    TC_BTree05() : TestCase("TC_BTree05") {;};
    void run();
};

void
TC_BTree05::run()
{
    ShortKey k;
    const size_t bufsize = 28;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initLeafPage(&buf[0]);

    PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.nKeyBytes == 1);

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uint64_t val;
    bool ok;
    uint32_t idx;

    val = 2;
    a_key = 2;
    ok = b.blockInsertInLeaf(&l, &pa, &a_key, val, &err);
    ASSERT_TRUE(ok == true);

    val = 1;
    a_key = 1;
    ok = b.blockInsertInLeaf(&l, &pa, &a_key, val, &err);
    ASSERT_TRUE(ok == true);

    a_key = 2;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 1);
    ASSERT_TRUE(pa.values[idx] == 2);
 
    a_key = 1;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 0);
    ASSERT_TRUE(pa.values[idx] == 1);

    a_key = 0;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == false);

    a_key = 3;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree06 : public TestCase {
    TC_BTree06() : TestCase("TC_BTree06") {;};
    void run();
};

void
TC_BTree06::run()
{
    ShortKey k;
    // want 3 leaf keys: hdr=8 + val=8*3 + key=1*3 = 33
    const size_t bufsize = 35;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);
    ASSERT_TRUE(ih.maxNumLeafKeys >= 3);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initLeafPage(&buf[0]);

    PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.nKeyBytes == 1);

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uint64_t val;
    bool ok;
    uint32_t idx;

    val = 10;
    a_key = 10;
    ok = b.blockInsertInLeaf(&l, &pa, &a_key, val, &err);
    ASSERT_TRUE(ok == true);

    val = 5;
    a_key = 5;
    ok = b.blockInsertInLeaf(&l, &pa, &a_key, val, &err);
    ASSERT_TRUE(ok == true);

    val = 3;
    a_key = 3;
    ok = b.blockInsertInLeaf(&l, &pa, &a_key, val, &err);
    ASSERT_TRUE(ok == true);

    a_key = 5;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 1);
    ASSERT_TRUE(pa.values[idx] == 5);

    a_key = 10;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 2);
    ASSERT_TRUE(pa.values[idx] == 10);

    a_key = 3;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 0);
    ASSERT_TRUE(pa.values[idx] == 3);

    a_key = 0;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == false);

    a_key = 11;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == false);

    a_key = 4;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == false);

    a_key = 6;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree07 : public TestCase {
    TC_BTree07() : TestCase("TC_BTree07") {;};
    void run();
};

void
TC_BTree07::run()
{
    ShortKey k;
    const size_t bufsize = 28;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initLeafPage(&buf[0]);

    PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.nKeyBytes == 1);

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uint64_t val;
    bool ok;
    uint32_t idx;

    val = 2;
    a_key = 2;
    ok = b.blockInsertInLeaf(&l, &pa, &a_key, val, &err);
    ASSERT_TRUE(ok == true);

    val = 1;
    a_key = 1;
    ok = b.blockInsertInLeaf(&l, &pa, &a_key, val, &err);
    ASSERT_TRUE(ok == true);

    a_key = 2;
    ok = b.blockDeleteFromLeaf(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == true);

    ok = b.blockDeleteFromLeaf(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == false);
    
    a_key = 1;
    ok = b.blockDeleteFromLeaf(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == true);

    ok = b.blockDeleteFromLeaf(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {


static void *tc_btree08_do_insert(void *);
static void *tc_btree08_do_delete(void *);

struct TC_BTree08 : public TestCase {
    BTree b;
    boost::shared_mutex l;
    PageAccess pa;
    
    TC_BTree08() : TestCase("TC_BTree08") {;};
    void run();
};

void
TC_BTree08::run()
{
    ShortKey k;
    const size_t bufsize = 28;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);

    this->b.header = &ih;
    this->b.root = NULL;
    this->b.ki = &k;

    uint8_t buf[bufsize];
    this->b.initLeafPage(&buf[0]);

    this->b.initPageAccess(&this->pa, &buf[0]);

    ASSERT_TRUE(ih.nKeyBytes == 1);

    pthread_attr_t a;
    int err;
    
    err = pthread_attr_init(&a);
    ASSERT_TRUE(err == 0);
    err = pthread_attr_setdetachstate(&a, PTHREAD_CREATE_JOINABLE);
    ASSERT_TRUE(err == 0);
    
    pthread_t t1, t2;
    err = pthread_create(&t1, &a, tc_btree08_do_delete, this);
    ASSERT_TRUE(err == 0);
    err = pthread_create(&t2, &a, tc_btree08_do_insert, this);
    ASSERT_TRUE(err == 0);

    void *junk;
    err = pthread_join(t1, &junk);
    ASSERT_TRUE(err == 0);
    err = pthread_join(t2, &junk);
    ASSERT_TRUE(err == 0);

    this->setStatus(true);
}

static void *
tc_btree08_do_insert(void *ptr)
{
    static int count = 0;
    uint8_t key;
    bool ok;
    uint64_t val;
    ErrorInfo err;

    TC_BTree08 *tc = reinterpret_cast<TC_BTree08 *>(ptr);
    key = 1;
    val = 1;
    while (count < 5) {
	ok = tc->b.blockInsertInLeaf(&tc->l, &tc->pa, &key, val, &err);
	if (ok)
	    count++;
    }
    
    return NULL;
}

static void *
tc_btree08_do_delete(void *ptr)
{
    static int count = 0;
    bool ok;
    ErrorInfo err;
    uint8_t key;

    TC_BTree08 *tc = reinterpret_cast<TC_BTree08 *>(ptr);
    key = 1;
    while (count < 5) {
	ok = tc->b.blockDeleteFromLeaf(&tc->l, &tc->pa, &key, &err) ;
	if (ok)
	    count++;
    }

    return NULL;
}

}

/************/

namespace dback {

struct TC_BTree09 : public TestCase {
    TC_BTree09() : TestCase("TC_BTree09") {;};
    void run();
};

void
TC_BTree09::run()
{
    ShortKey k;
    // want 3 leaf keys: hdr=8 + val=8*3 + key=1*3 = 33
    const size_t bufsize = 35;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);
    ASSERT_TRUE(ih.maxNumLeafKeys >= 3);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initLeafPage(&buf[0]);

    PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.nKeyBytes == 1);

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uint64_t val;
    bool ok;
    uint32_t idx;

    val = 10;
    a_key = 10;
    ok = b.blockInsertInLeaf(&l, &pa, &a_key, val, &err);
    ASSERT_TRUE(ok == true);

    val = 5;
    a_key = 5;
    ok = b.blockInsertInLeaf(&l, &pa, &a_key, val, &err);
    ASSERT_TRUE(ok == true);

    val = 3;
    a_key = 3;
    ok = b.blockInsertInLeaf(&l, &pa, &a_key, val, &err);
    ASSERT_TRUE(ok == true);

    a_key = 5;
    ok = b.blockFindInLeaf(&l, &pa, &a_key, &val, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val == 5);

    a_key = 10;
    ok = b.blockFindInLeaf(&l, &pa, &a_key, &val, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val == 10);

    a_key = 3;
    ok = b.blockFindInLeaf(&l, &pa, &a_key, &val, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val == 3);

    a_key = 0;
    ok = b.blockFindInLeaf(&l, &pa, &a_key, &val, &err);
    ASSERT_TRUE(ok == false);

    a_key = 11;
    ok = b.blockFindInLeaf(&l, &pa, &a_key, &val, &err);
    ASSERT_TRUE(ok == false);

    a_key = 4;
    ok = b.blockFindInLeaf(&l, &pa, &a_key, &val, &err);
    ASSERT_TRUE(ok == false);

    a_key = 6;
    ok = b.blockFindInLeaf(&l, &pa, &a_key, &val, &err);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree10 : public TestCase {
    TC_BTree10() : TestCase("TC_BTree10") {;};
    void run();
};

void
TC_BTree10::run()
{
    ShortKey k;
    const size_t bufsize = 28;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);
    ASSERT_TRUE(ih.maxNumLeafKeys > 1);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initNonLeafPage(&buf[0]);

    PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.nKeyBytes == 1);

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uint32_t child;
    bool ok;
    uint32_t idx;

    child = 1;
    a_key = 1;
    ok = b.blockInsertInNonLeaf(&l, &pa, &a_key, child, &err);
    ASSERT_TRUE(ok == true);

    child = 2;
    a_key = 2;
    ok = b.blockInsertInNonLeaf(&l, &pa, &a_key, child, &err);
    ASSERT_TRUE(ok == true);

    a_key = 1;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 0);
    ASSERT_TRUE(pa.childPtrs[idx] == 1);
 
    a_key = 2;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 1);
    ASSERT_TRUE(pa.childPtrs[idx] == 2);

    this->setStatus(true);
}

}


/************/

namespace dback {

struct TC_BTree11 : public TestCase {
    TC_BTree11() : TestCase("TC_BTree11") {;};
    void run();
};

void
TC_BTree11::run()
{
    ShortKey k;
    const size_t bufsize = 28;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initNonLeafPage(&buf[0]);

    PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.nKeyBytes == 1);

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uint32_t child;
    bool ok;
    uint32_t idx;

    child = 2;
    a_key = 2;
    ok = b.blockInsertInNonLeaf(&l, &pa, &a_key, child, &err);
    ASSERT_TRUE(ok == true);

    child = 1;
    a_key = 1;
    ok = b.blockInsertInNonLeaf(&l, &pa, &a_key, child, &err);
    ASSERT_TRUE(ok == true);

    a_key = 2;
    ok = b.blockDeleteFromNonLeaf(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == true);

    ok = b.blockDeleteFromNonLeaf(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == false);
    
    a_key = 1;
    ok = b.blockDeleteFromNonLeaf(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == true);

    ok = b.blockDeleteFromNonLeaf(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree12 : public TestCase {
    TC_BTree12() : TestCase("TC_BTree12") {;};
    void run();
};

void
TC_BTree12::run()
{
    ShortKey k;
    // want 3 leaf keys: hdr=8 + val=8*3 + key=1*3 = 33
    const size_t bufsize = 35;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);
    ASSERT_TRUE(ih.maxNumLeafKeys >= 3);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initNonLeafPage(&buf[0]);

    PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.nKeyBytes == 1);

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uint32_t child;
    bool ok;
    uint32_t idx;

    child = 10;
    a_key = 10;
    ok = b.blockInsertInNonLeaf(&l, &pa, &a_key, child, &err);
    ASSERT_TRUE(ok == true);

    child = 5;
    a_key = 5;
    ok = b.blockInsertInNonLeaf(&l, &pa, &a_key, child, &err);
    ASSERT_TRUE(ok == true);

    child = 3;
    a_key = 3;
    ok = b.blockInsertInNonLeaf(&l, &pa, &a_key, child, &err);
    ASSERT_TRUE(ok == true);

    a_key = 5;
    ok = b.blockFindInNonLeaf(&l, &pa, &a_key, &child, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(child == 5);

    a_key = 10;
    ok = b.blockFindInNonLeaf(&l, &pa, &a_key, &child, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(child == 10);

    a_key = 3;
    ok = b.blockFindInNonLeaf(&l, &pa, &a_key, &child, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(child == 3);

    a_key = 0;
    ok = b.blockFindInNonLeaf(&l, &pa, &a_key, &child, &err);
    ASSERT_TRUE(ok == false);

    a_key = 11;
    ok = b.blockFindInNonLeaf(&l, &pa, &a_key, &child, &err);
    ASSERT_TRUE(ok == false);

    a_key = 4;
    ok = b.blockFindInNonLeaf(&l, &pa, &a_key, &child, &err);
    ASSERT_TRUE(ok == false);

    a_key = 6;
    ok = b.blockFindInNonLeaf(&l, &pa, &a_key, &child, &err);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree13 : public TestCase {
    TC_BTree13() : TestCase("TC_BTree13") {;};
    void run();
};

void
TC_BTree13::run()
{
    ShortKey k;
    // want 3 leaf keys: hdr=8 + val=8*3 + key=1*3 = 33
    const size_t bufsize = 35;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);
    ASSERT_TRUE(ih.maxNumLeafKeys >= 3);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t leaf_buf[bufsize];
    b.initLeafPage(&leaf_buf[0]);
    uint8_t non_leaf_buf[bufsize];
    b.initNonLeafPage(&non_leaf_buf[0]);

    PageAccess pa_leaf;
    b.initPageAccess(&pa_leaf, &leaf_buf[0]);
    PageAccess pa_non_leaf;
    b.initPageAccess(&pa_non_leaf, &non_leaf_buf[0]);

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    bool ok;
    uint32_t child;
    uint64_t data;

    a_key = 10;

    err.clear();
    ok = b.blockInsertInNonLeaf(&l, &pa_leaf, &a_key, 1, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);
    ASSERT_TRUE(err.haveError == true);
    ASSERT_TRUE(err.message.length() > 0);

    err.clear();
    ok = b.blockInsertInLeaf(&l, &pa_non_leaf, &a_key, 1, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);
    ASSERT_TRUE(err.haveError == true);
    ASSERT_TRUE(err.message.length() > 0);


    err.clear();
    ok = b.blockFindInNonLeaf(&l, &pa_leaf, &a_key, &child, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.haveError == true);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);
    ASSERT_TRUE(err.message.length() > 0);

    err.clear();
    ok = b.blockFindInLeaf(&l, &pa_non_leaf, &a_key, &data, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.haveError == true);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);
    ASSERT_TRUE(err.message.length() > 0);

    a_key = 0;
    ok = true;
    while (ok) {
	err.clear();
	ok = b.blockInsertInNonLeaf(&l, &pa_non_leaf, &a_key, 1, &err);
	if (ok)
	    a_key++;
    }
    ASSERT_TRUE(err.haveError == true);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_NODE_FULL);
    ASSERT_TRUE(err.message.find("full") != std::string::npos);

    err.clear();
    a_key = 0;
    ok = b.blockDeleteFromNonLeaf(&l, &pa_non_leaf, &a_key, &err);
    ASSERT_TRUE(ok == true);

    err.clear();
    a_key = 0;
    ok = b.blockDeleteFromNonLeaf(&l, &pa_leaf, &a_key, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.haveError == true);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);

    err.clear();
    a_key = 1;
    ok = b.blockInsertInNonLeaf(&l, &pa_non_leaf, &a_key, 1, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.haveError == true);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_DUPLICATE_INSERT);
    ASSERT_TRUE(err.message.find("duplicate") != std::string::npos);
    
    a_key = 0;
    ok = true;
    while (ok) {
	err.clear();
	ok = b.blockInsertInLeaf(&l, &pa_leaf, &a_key, 1, &err);
	if (ok)
	    a_key++;
    }
    ASSERT_TRUE(err.haveError == true);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_NODE_FULL);
    ASSERT_TRUE(err.message.find("full") != std::string::npos);

    err.clear();
    a_key = 0;
    ok = b.blockDeleteFromLeaf(&l, &pa_non_leaf, &a_key, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.haveError == true);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);
    ASSERT_TRUE(err.message.length() > 0);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree14 : public TestCase {
    TC_BTree14() : TestCase("TC_BTree14") {;};
    void run();
};

void
TC_BTree14::run()
{
    ShortKey k;
    // want 3 leaf keys: hdr=8 + val=8*3 + key=1*3 = 33
    const size_t bufsize = 35;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);
    ASSERT_TRUE(ih.maxNumLeafKeys >= 3);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t nl_b1[bufsize];
    b.initNonLeafPage(&nl_b1[0]);
    uint8_t nl_b2[bufsize];
    b.initNonLeafPage(&nl_b2[0]);

    uint8_t l_b3[bufsize];
    b.initLeafPage(&l_b3[0]);
    uint8_t l_b4[bufsize];
    b.initLeafPage(&l_b4[0]);

    PageAccess nl_pa1;
    b.initPageAccess(&nl_pa1, &nl_b1[0]);
    PageAccess nl_pa2;
    b.initPageAccess(&nl_pa2, &nl_b2[0]);
    PageAccess l_pa3;
    b.initPageAccess(&l_pa3, &l_b3[0]);
    PageAccess l_pa4;
    b.initPageAccess(&l_pa4, &l_b4[0]);

    uint8_t key;
    ErrorInfo err;
    bool ok;
    boost::shared_mutex l;

    err.clear();
    ok = b.splitLeaf(NULL, &nl_pa2, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitLeaf(&nl_pa1, &nl_pa2, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitLeaf(&l_pa3, &nl_pa2, &key, &err);
    ASSERT_TRUE(ok == false);

    uint64_t val;

    key = 0;
    val = 0;
    while (true) {
	err.clear();
	ok = b.blockInsertInLeaf(&l, &l_pa3, &key, val, &err);
	if (!ok)
	    break;
	key++;
	val++;
    }

    err.clear();
    ok = b.splitLeaf(&l_pa3, NULL, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitLeaf(&l_pa3, &l_pa3, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitLeaf(&l_pa3, &l_pa4, NULL, &err);
    ASSERT_TRUE(ok == false);
    
    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree15 : public TestCase {
    TC_BTree15() : TestCase("TC_BTree15") {;};
    void run();
};

void
TC_BTree15::run()
{
    ShortKey k;
    const size_t bufsize = 35;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);
    ASSERT_TRUE(ih.maxNumLeafKeys >= 3);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf1[bufsize];
    b.initLeafPage(&buf1[0]);
    uint8_t buf2[bufsize];
    b.initLeafPage(&buf2[0]);

    PageAccess p1;
    b.initPageAccess(&p1, &buf1[0]);
    PageAccess p2;
    b.initPageAccess(&p2, &buf2[0]);

    uint64_t val;
    uint8_t key, mid;
    ErrorInfo err;
    bool ok;
    boost::shared_mutex l;

    key = 0;
    val = 0;
    while (true) {
	err.clear();
	ok = b.blockInsertInLeaf(&l, &p1, &key, val, &err);
	if (!ok)
	    break;
	key++;
	val++;
    }
    ASSERT_TRUE(key == ih.maxNumLeafKeys);

    ok = b.splitLeaf(&p1, &p2, &mid, &err);
    ASSERT_TRUE(ok == true);
    
    for (uint8_t k2 = 0; k2 < mid; k2++) {
	ok = b.blockFindInLeaf(&l, &p1, &k2, &val, &err);
	ASSERT_TRUE(ok == true);
    }
    for (uint8_t k2 = mid; k2 < ih.maxNumLeafKeys; k2++) {
	err.clear();
	ok = b.blockFindInLeaf(&l, &p1, &k2, &val, &err);
	ASSERT_TRUE(ok == false);
	ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_KEY_NOT_FOUND);
    }

    for (uint8_t k2 = 0; k2 < mid; k2++) {
	err.clear();
	ok = b.blockFindInLeaf(&l, &p2, &k2, &val, &err);
	ASSERT_TRUE(ok == false);
	ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_KEY_NOT_FOUND);
    }
    for (uint8_t k2 = mid; k2 < ih.maxNumLeafKeys; k2++) {
	ok = b.blockFindInLeaf(&l, &p2, &k2, &val, &err);
	ASSERT_TRUE(ok == true);
    }

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree16 : public TestCase {
    TC_BTree16() : TestCase("TC_BTree16") {;};
    void run();
};

void
TC_BTree16::run()
{
    ShortKey k;
    // want 3 leaf keys: hdr=8 + val=8*3 + key=1*3 = 33
    const size_t bufsize = 35;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);
    ASSERT_TRUE(ih.maxNumLeafKeys >= 3);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t nl_b1[bufsize];
    b.initNonLeafPage(&nl_b1[0]);
    uint8_t nl_b2[bufsize];
    b.initNonLeafPage(&nl_b2[0]);

    uint8_t l_b3[bufsize];
    b.initLeafPage(&l_b3[0]);
    uint8_t l_b4[bufsize];
    b.initLeafPage(&l_b4[0]);

    PageAccess nl_pa1;
    b.initPageAccess(&nl_pa1, &nl_b1[0]);
    PageAccess nl_pa2;
    b.initPageAccess(&nl_pa2, &nl_b2[0]);
    PageAccess l_pa3;
    b.initPageAccess(&l_pa3, &l_b3[0]);
    PageAccess l_pa4;
    b.initPageAccess(&l_pa4, &l_b4[0]);

    uint8_t key;
    ErrorInfo err;
    bool ok;
    boost::shared_mutex l;

    err.clear();
    ok = b.splitNonLeaf(NULL, &l_pa3, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitNonLeaf(&l_pa3, &l_pa3, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitNonLeaf(&nl_pa1, &l_pa3, &key, &err);
    ASSERT_TRUE(ok == false);

    uint32_t child;

    key = 0;
    child = 0;
    while (true) {
	err.clear();
	ok = b.blockInsertInNonLeaf(&l, &nl_pa1, &key, child, &err);
	if (!ok)
	    break;
	key++;
	child++;
    }

    err.clear();
    ok = b.splitNonLeaf(&nl_pa1, NULL, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitNonLeaf(&nl_pa1, &l_pa3, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitNonLeaf(&nl_pa1, &nl_pa2, NULL, &err);
    ASSERT_TRUE(ok == false);
    
    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree17 : public TestCase {
    TC_BTree17() : TestCase("TC_BTree17") {;};
    void run();
};

void
TC_BTree17::run()
{
    ShortKey k;
    const size_t bufsize = 35;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumNLeafKeys >= 2);
    ASSERT_TRUE(ih.minNumNLeafKeys > 0);
    ASSERT_TRUE(ih.maxNumLeafKeys >= 3);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf1[bufsize];
    b.initNonLeafPage(&buf1[0]);
    uint8_t buf2[bufsize];
    b.initNonLeafPage(&buf2[0]);

    PageAccess p1;
    b.initPageAccess(&p1, &buf1[0]);
    PageAccess p2;
    b.initPageAccess(&p2, &buf2[0]);

    uint32_t val;
    uint8_t key, mid;
    ErrorInfo err;
    bool ok;
    boost::shared_mutex l;

    key = 0;
    val = 0;
    while (true) {
	err.clear();
	ok = b.blockInsertInNonLeaf(&l, &p1, &key, val, &err);
	if (!ok)
	    break;
	key++;
	val++;
    }
    ASSERT_TRUE(key == ih.maxNumNLeafKeys);

    ok = b.splitNonLeaf(&p1, &p2, &mid, &err);
    ASSERT_TRUE(ok == true);
    
    for (uint8_t k2 = 0; k2 < mid; k2++) {
	ok = b.blockFindInNonLeaf(&l, &p1, &k2, &val, &err);
	ASSERT_TRUE(ok == true);
    }
    for (uint8_t k2 = mid; k2 < ih.maxNumNLeafKeys; k2++) {
	err.clear();
	ok = b.blockFindInNonLeaf(&l, &p1, &k2, &val, &err);
	ASSERT_TRUE(ok == false);
	ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_KEY_NOT_FOUND);
    }

    for (uint8_t k2 = 0; k2 < mid; k2++) {
	err.clear();
	ok = b.blockFindInNonLeaf(&l, &p2, &k2, &val, &err);
	ASSERT_TRUE(ok == false);
	ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_KEY_NOT_FOUND);
    }
    for (uint8_t k2 = mid; k2 < ih.maxNumNLeafKeys; k2++) {
	ok = b.blockFindInNonLeaf(&l, &p2, &k2, &val, &err);
	ASSERT_TRUE(ok == true);
    }

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree18 : public TestCase {
    TC_BTree18() : TestCase("TC_BTree18") {;};
    void run();
};

void
TC_BTree18::run()
{
    const int n_keys = 20;
    const size_t bufsize = sizeof(PageHeader)
	+ n_keys * (sizeof(uint64_t) + 1); /* keysize */
    ShortKey k;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumLeafKeys == n_keys);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf1[bufsize];
    b.initLeafPage(&buf1[0]);
    uint8_t buf2[bufsize];
    b.initLeafPage(&buf2[0]);

    PageAccess p1;
    b.initPageAccess(&p1, &buf1[0]);
    PageAccess p2;
    b.initPageAccess(&p2, &buf2[0]);

    uint64_t val;
    uint8_t key;
    ErrorInfo err;
    bool ok;
    boost::shared_mutex l;

    key = 0;
    val = 0;
    while (key < 15) {
	err.clear();
	ok = b.blockInsertInLeaf(&l, &p1, &key, val, &err);
	ASSERT_TRUE(ok == true);
	ok = b.blockInsertInLeaf(&l, &p2, &key, val, &err);
	ASSERT_TRUE(ok == true);
	key++;
	val++;
    }

    bool dstIsFirst = true;

    err.clear();
    ok = b.concatLeaf(NULL, &p2, dstIsFirst, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);

    err.clear();
    ok = b.concatLeaf(&p1, NULL, dstIsFirst, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);

    err.clear();
    ok = b.concatLeaf(&p1, &p2, dstIsFirst, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_BTree19 : public TestCase {
    TC_BTree19() : TestCase("TC_BTree19") {;};
    void run();
};

void
TC_BTree19::run()
{
    const int n_keys = 20;
    const size_t bufsize = sizeof(PageHeader)
	+ n_keys * (sizeof(uint64_t) + 1); /* keysize */
    ShortKey k;
    IndexHeader ih;
    k.initIndexHeader(&ih, bufsize);
    ASSERT_TRUE(ih.maxNumLeafKeys == n_keys);

    BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf1[bufsize];
    b.initLeafPage(&buf1[0]);
    uint8_t buf2[bufsize];
    b.initLeafPage(&buf2[0]);

    PageAccess p1;
    b.initPageAccess(&p1, &buf1[0]);
    PageAccess p2;
    b.initPageAccess(&p2, &buf2[0]);

    uint64_t val;
    uint8_t key, key2;
    ErrorInfo err;
    bool ok;
    boost::shared_mutex l;

    key = 0;
    val = 0;
    while (key < 10) {

	err.clear();
	ok = b.blockInsertInLeaf(&l, &p1, &key, val, &err);
	ASSERT_TRUE(ok == true);

	key2 = key + 100;
	err.clear();
	ok = b.blockInsertInLeaf(&l, &p2, &key2, val, &err);
	ASSERT_TRUE(ok == true);

	key++;
	val++;
    }

    bool dstIsFirst = true;

    err.clear();
    ok = b.concatLeaf(&p1, &p2, dstIsFirst, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(p2.header->numKeys == 0);
    ASSERT_TRUE(p1.header->numKeys == 20);

    for (key = 0; key < 10; key++) {
	key2 = key + 100;

	err.clear();
	ok = b.blockFindInLeaf(&l, &p1, &key, &val, &err);
	ASSERT_TRUE(ok == true);

	err.clear();
	ok = b.blockFindInLeaf(&l, &p1, &key2, &val, &err);
	ASSERT_TRUE(ok == true);
    }

    p1.header->numKeys = 0;
    p2.header->numKeys = 0;

    key = 0;
    val = 0;
    while (key < 10) {

	key2 = key + 100;

	err.clear();
	ok = b.blockInsertInLeaf(&l, &p1, &key2, val, &err);
	ASSERT_TRUE(ok == true);

	err.clear();
	ok = b.blockInsertInLeaf(&l, &p2, &key, val, &err);
	ASSERT_TRUE(ok == true);

	key++;
	val++;
    }
    
    dstIsFirst = false;

    err.clear();
    ok = b.concatLeaf(&p1, &p2, dstIsFirst, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(p2.header->numKeys == 0);
    ASSERT_TRUE(p1.header->numKeys == 20);

    for (key = 0; key < 10; key++) {
	key2 = key + 100;

	err.clear();
	ok = b.blockFindInLeaf(&l, &p1, &key, &val, &err);
	ASSERT_TRUE(ok == true);

	err.clear();
	ok = b.blockFindInLeaf(&l, &p1, &key2, &val, &err);
	ASSERT_TRUE(ok == true);
    }

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree00 : public TestCase {
    TC_R2BTree00() : TestCase("TC_R2BTree00") {;};
    void run();
};

void
TC_R2BTree00::run()
{
    R2BTree b;
    R2IndexHeader ih;
    R2PageHeader ph;
    R2PageAccess pa;
    R2UUIDKey k;
    ErrorInfo err;
    bool ok;

    ih.keySize = 16;
    ih.pageSize = 4096;
    ih.maxNumKeys[PageTypeNonLeaf] = 0;
    ih.minNumKeys[PageTypeNonLeaf] = 0;
    ih.maxNumKeys[PageTypeLeaf] = 0;
    ih.minNumKeys[PageTypeLeaf] = 0;

    ph.parentPageNum = 0;
    ph.numKeys = 0;
    ph.pageType = PageTypeLeaf;
    ph.pad = 0;

    pa.header = &ph;
    pa.keys = NULL;
    pa.vals = NULL;

    b.header = &ih;
    b.root = &pa;
    b.ki = &k;

    uint8_t *key = NULL;
    uint8_t *val = NULL;

    boost::shared_mutex m;

    ok = b.blockInsert(&m, &pa, key, val, &err);
    ASSERT_TRUE(ok == false);
    
    ph.pageType = PageTypeNonLeaf;
    ok = b.blockInsert(&m, &pa, key, val, &err);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree01 : public TestCase {
    TC_R2BTree01() : TestCase("TC_R2BTree01") {;};
    void run();
};

void
TC_R2BTree01::run()
{
    {
	R2IndexHeader ih;
	R2BTreeParams p;

	p.pageSize = 76;
	p.keySize = 16;
	p.valSize =  8;
	// page = <pageheader><vals><keys>
	// <pageheader> = 8 bytes
	// leaf: sz = 8 + 2 *(8 + 16) = 56; 
	// non leaf: sz = 8 + 3 * ( 8 + 4 ) + 4 = 48
	// but non leaf size must be even
	R2BTree::initIndexHeader(&ih, &p);
	ASSERT_TRUE(ih.keySize == 16);
	ASSERT_TRUE(ih.pageSize == 76);
	ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] == 2);
	ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] == 1);
	ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] == 2);
	ASSERT_TRUE(ih.minNumKeys[PageTypeLeaf] == 1);
    }

    {
	R2IndexHeader ih;
	R2BTreeParams p;

	p.pageSize = 80;
	p.keySize = 16;
	p.valSize =  8;

	R2BTree::initIndexHeader(&ih, &p);
	ASSERT_TRUE(ih.keySize == 16);
	ASSERT_TRUE(ih.pageSize == 80);

	// leaf: sz=8 + 3*(16 + 8) = 80
	// leaf sizes must be even
	ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] == 2);
	ASSERT_TRUE(ih.minNumKeys[PageTypeLeaf] == 1);

	// non leaf: sz=8 + 3 * (16 + 4) + 4 = 88
	// non leaf must be even
	ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] == 2);
	ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] == 1);
    }

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree02 : public TestCase {
    TC_R2BTree02() : TestCase("TC_R2BTree02") {;};
    void run();
};

void
TC_R2BTree02::run()
{
    uint8_t buf[4096];
    R2BTree b;
    R2PageAccess pa;
    R2IndexHeader ih;
    uint32_t idx;
    bool found;
    R2UUIDKey k;
    R2BTreeParams p;

    p.pageSize = sizeof(buf);
    p.keySize = 16;
    p.valSize = 8;
    R2BTree::initIndexHeader(&ih, &p);

    uint8_t key[ ih.keySize ];

    memset(&buf[0], 0, sizeof(buf));
    pa.header = reinterpret_cast<R2PageHeader *>(&buf[0]);
    pa.header->pageType = PageTypeLeaf;
    pa.keys = NULL;
    pa.vals = NULL;

    b.header = &ih;
    b.root = &pa;
    b.ki = &k;

    for (uint32_t i = 0; i < ih.keySize; i++)
	key[i] = 0;

    idx = 2;

    found = b.findKeyPosition(&pa, &key[0], &idx);
    ASSERT_TRUE(found == false);
    ASSERT_TRUE(idx == 0);

    this->setStatus(true);
}

}

/************/

namespace dback {

/**
 * Simple test class for 1 byte keys.
 */
class R2ShortKey : public R2KeyInterface {
public:
    int compare(const uint8_t *a, const uint8_t *b);
};

int
R2ShortKey::compare(const uint8_t *a, const uint8_t *b)
{
    if (*a < *b)
	return -1;
    else if (*a > *b)
	return 1;
    return 0;
}

struct TC_R2BTree03 : public TestCase {
    TC_R2BTree03() : TestCase("TC_R2BTree03") {;};
    void run();
};

void
TC_R2BTree03::run()
{
    R2ShortKey k;
    const size_t bufsize = 28;
    R2IndexHeader ih;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] > 1);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initLeafPage(&buf[0]);

    R2PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    uint8_t a_key[ih.keySize];
    memset(&a_key[0], 99, sizeof(a_key));

    ErrorInfo err;
    boost::shared_mutex l;
    uint64_t val = 97;
    bool ok;
    uint8_t *vptr = reinterpret_cast<uint8_t *>(&val);

    ok = b.blockInsert(&l, &pa, &a_key[0], vptr, &err);
    ASSERT_TRUE(ok == true);

    uint32_t idx = 5;
    ok = b.findKeyPosition(&pa, &a_key[0], &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 0);

    uint64_t data;
    b.getData(reinterpret_cast<uint8_t *>(&data), &pa, idx);
    ASSERT_TRUE(data == 97);

    ASSERT_TRUE(pa.keys[0] == 99);

    vptr = reinterpret_cast<uint8_t *>(&val);
    ok = b.blockInsert(&l, &pa, &a_key[0], vptr, &err);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree04 : public TestCase {
    TC_R2BTree04() : TestCase("TC_R2BTree04") {;};
    void run();
};

void
TC_R2BTree04::run()
{
    const size_t bufsize = 28;
    R2BTreeParams params;

    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);

    R2ShortKey k;

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initLeafPage(&buf[0]);

    R2PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.keySize == 1);

    union uv64 {
	uint64_t val64;
	uint8_t  val8;
    };

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uv64 val;
    bool ok;
    uint32_t idx;

    val.val64 = 1;
    a_key = 1;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    val.val64 = 2;
    a_key = 2;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    uv64 val2;
    a_key = 1;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 0);
    ok = b.getData(&val2.val8, &pa, idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val2.val64 == 1);
 
    a_key = 2;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 1);
    ok = b.getData(&val2.val8, &pa, idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val2.val64 == 2);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree05 : public TestCase {
    TC_R2BTree05() : TestCase("TC_R2BTree05") {;};
    void run();
};

void
TC_R2BTree05::run()
{
    R2ShortKey k;
    const size_t bufsize = 28;

    R2BTreeParams params;

    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initLeafPage(&buf[0]);

    R2PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.keySize == 1);

    union uv64 {
	uint64_t val64;
	uint8_t  val8;
    };

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uv64 val;
    bool ok;
    uint32_t idx;

    val.val64 = 2;
    a_key = 2;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    val.val64 = 1;
    a_key = 1;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    a_key = 2;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 1);
    ok = b.getData(&val.val8, &pa, idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val64 == 2);
 
    a_key = 1;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 0);
    ok = b.getData(&val.val8, &pa, idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val64 == 1);

    a_key = 0;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == false);

    a_key = 3;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree06 : public TestCase {
    TC_R2BTree06() : TestCase("TC_R2BTree06") {;};
    void run();
};

void
TC_R2BTree06::run()
{
    R2ShortKey k;
    // want 3 leaf keys: hdr=8 + val=8*3 + key=1*3 = 33
    const size_t bufsize = 45;
    R2IndexHeader ih;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] >= 3);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initLeafPage(&buf[0]);

    R2PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.keySize == 1);

    union uv64 {
	uint64_t val64;
	uint8_t  val8;
    };

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uv64 val;
    bool ok;
    uint32_t idx;

    val.val64 = 10;
    a_key = 10;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    val.val64 = 5;
    a_key = 5;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    val.val64 = 3;
    a_key = 3;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    a_key = 5;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 1);
    ok = b.getData(&val.val8, &pa, idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val64 == 5);

    ok = b.getData(&val.val8, &pa, idx + 200);
    ASSERT_TRUE(ok == false);
    ok = b.getData(NULL, &pa, idx);
    ASSERT_TRUE(ok == false);

    a_key = 10;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 2);
    ok = b.getData(&val.val8, &pa, idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val64 == 10);

    a_key = 3;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 0);
    ok = b.getData(&val.val8, &pa, idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val64 == 3);

    a_key = 0;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == false);

    a_key = 11;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == false);

    a_key = 4;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == false);

    a_key = 6;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree07 : public TestCase {
    TC_R2BTree07() : TestCase("TC_R2BTree07") {;};
    void run();
};

void
TC_R2BTree07::run()
{
    R2ShortKey k;
    const size_t bufsize = 28;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initLeafPage(&buf[0]);

    R2PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.keySize == 1);

    union uv64 {
	uint64_t val64;
	uint8_t  val8;
    };

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uv64 val;
    bool ok;
    uint32_t idx;

    val.val64 = 2;
    a_key = 2;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    val.val64 = 1;
    a_key = 1;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    a_key = 2;
    ok = b.blockDelete(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == true);

    ok = b.blockDelete(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == false);
    
    a_key = 1;
    ok = b.blockDelete(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {


static void *tc_r2btree08_do_insert(void *);
static void *tc_r2btree08_do_delete(void *);

struct TC_R2BTree08 : public TestCase {
    R2BTree b;
    boost::shared_mutex l;
    R2PageAccess pa;
    
    TC_R2BTree08() : TestCase("TC_R2BTree08") {;};
    void run();
};

void
TC_R2BTree08::run()
{
    R2ShortKey k;
    const size_t bufsize = 280;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);

    ih.minNumKeys[PageTypeLeaf] = 0;
    ih.minNumKeys[PageTypeNonLeaf] = 0;

    this->b.header = &ih;
    this->b.root = NULL;
    this->b.ki = &k;

    uint8_t buf[bufsize];
    this->b.initLeafPage(&buf[0]);

    this->b.initPageAccess(&this->pa, &buf[0]);

    ASSERT_TRUE(ih.keySize == 1);

    pthread_attr_t a;
    int err;
    
    err = pthread_attr_init(&a);
    ASSERT_TRUE(err == 0);
    err = pthread_attr_setdetachstate(&a, PTHREAD_CREATE_JOINABLE);
    ASSERT_TRUE(err == 0);
    
    pthread_t t1, t2;
    err = pthread_create(&t1, &a, tc_r2btree08_do_delete, this);
    ASSERT_TRUE(err == 0);
    err = pthread_create(&t2, &a, tc_r2btree08_do_insert, this);
    ASSERT_TRUE(err == 0);

    void *junk;
    err = pthread_join(t1, &junk);
    ASSERT_TRUE(err == 0);
    err = pthread_join(t2, &junk);
    ASSERT_TRUE(err == 0);

    this->setStatus(true);
}

static void *
tc_r2btree08_do_insert(void *ptr)
{
    static int count = 0;
    uint8_t key;
    bool ok;
    ErrorInfo err;

    union uv64 {
	uint64_t val64;
	uint8_t  val8;
    };

    uv64 val;

    TC_R2BTree08 *tc = reinterpret_cast<TC_R2BTree08 *>(ptr);
    key = 1;
    val.val64 = 1;
    while (count < 5) {
	ok = tc->b.blockInsert(&tc->l, &tc->pa, &key, &val.val8, &err);
	if (ok)
	    count++;
    }
    
    return NULL;
}

static void *
tc_r2btree08_do_delete(void *ptr)
{
    static int count = 0;
    bool ok;
    ErrorInfo err;
    uint8_t key;

    TC_R2BTree08 *tc = reinterpret_cast<TC_R2BTree08 *>(ptr);
    key = 1;
    while (count < 5) {
	ok = tc->b.blockDelete(&tc->l, &tc->pa, &key, &err) ;
	if (ok)
	    count++;
    }

    return NULL;
}

}

/************/

namespace dback {

struct TC_R2BTree09 : public TestCase {
    TC_R2BTree09() : TestCase("TC_R2BTree09") {;};
    void run();
};

void
TC_R2BTree09::run()
{
    R2ShortKey k;
    // want 3 leaf keys: hdr=8 + val=8*3 + key=1*3 = 33
    const size_t bufsize = 80;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] >= 3);

    ih.maxNumKeys[PageTypeLeaf] = 3;

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initLeafPage(&buf[0]);

    R2PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.keySize == 1);

    union uv64 {
	uint64_t val64;
	uint8_t  val8;
    };

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uv64 val;
    bool ok;
    uint32_t idx;

    val.val64 = 10;
    a_key = 10;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    val.val64 = 5;
    a_key = 5;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    val.val64 = 3;
    a_key = 3;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    a_key = 5;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val64 == 5);

    a_key = 10;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val64 == 10);

    a_key = 3;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val64 == 3);

    a_key = 0;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == false);

    a_key = 11;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == false);

    a_key = 4;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == false);

    a_key = 6;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == false);

    a_key = 10;
    ok = b.blockFind(&l, &pa, &a_key, NULL, &err);
    ASSERT_TRUE(ok == true);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree10 : public TestCase {
    TC_R2BTree10() : TestCase("TC_R2BTree10") {;};
    void run();
};

void
TC_R2BTree10::run()
{
    R2ShortKey k;
    const size_t bufsize = 28;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] > 1);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initNonLeafPage(&buf[0]);

    R2PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.keySize == 1);

    union uv {
	uint64_t val64;
	uint32_t val32;
	uint8_t  val8;
    };

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uv val;
    bool ok;
    uint32_t idx;

    val.val32 = 1;
    a_key = 1;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    val.val32 = 2;
    a_key = 2;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    a_key = 1;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 0);

    ok = b.getData(&val.val8, &pa, idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val32 == 1);
 
    a_key = 2;
    ok = b.findKeyPosition(&pa, &a_key, &idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(idx == 1);

    ok = b.getData(&val.val8, &pa, idx);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val32 == 2);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree11 : public TestCase {
    TC_R2BTree11() : TestCase("TC_R2BTree11") {;};
    void run();
};

void
TC_R2BTree11::run()
{
    R2ShortKey k;
    const size_t bufsize = 28;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);

    ih.minNumKeys[PageTypeLeaf] = 0;
    ih.minNumKeys[PageTypeNonLeaf] = 0;

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initNonLeafPage(&buf[0]);

    R2PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.keySize == 1);

    union uv {
	uint64_t val64;
	uint32_t val32;
	uint8_t  val8;
    };

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uv val;
    bool ok;
    uint32_t idx;

    val.val32 = 2;
    a_key = 2;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    val.val32 = 1;
    a_key = 1;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    a_key = 2;
    ok = b.blockDelete(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == true);

    ok = b.blockDelete(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == false);
    
    a_key = 1;
    ok = b.blockDelete(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == true);

    ok = b.blockDelete(&l, &pa, &a_key, &err);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree12 : public TestCase {
    TC_R2BTree12() : TestCase("TC_R2BTree12") {;};
    void run();
};

void
TC_R2BTree12::run()
{
    R2ShortKey k;
    // want 3 leaf keys: hdr=8 + val=8*3 + key=1*3 = 33
    const size_t bufsize = 200;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] >= 3);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf[bufsize];
    b.initNonLeafPage(&buf[0]);

    R2PageAccess pa;
    b.initPageAccess(&pa, &buf[0]);

    ASSERT_TRUE(ih.keySize == 1);

    union uv {
	uint64_t val64;
	uint32_t val32;
	uint8_t  val8;
    };

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    uv val;
    bool ok;
    uint32_t idx;

    val.val32 = 10;
    a_key = 10;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    val.val32 = 5;
    a_key = 5;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    val.val32 = 3;
    a_key = 3;
    ok = b.blockInsert(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);

    a_key = 5;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val32 == 5);

    a_key = 10;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val32 == 10);

    a_key = 3;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val32 == 3);

    a_key = 0;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == false);

    a_key = 11;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == false);

    a_key = 4;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == false);

    a_key = 6;
    ok = b.blockFind(&l, &pa, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree13 : public TestCase {
    TC_R2BTree13() : TestCase("TC_R2BTree13") {;};
    void run();
};

void
TC_R2BTree13::run()
{
    R2ShortKey k;
    // want 3 leaf keys: hdr=8 + val=8*3 + key=1*3 = 33
    const size_t bufsize = 350;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] >= 3);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t leaf_buf[bufsize];
    b.initLeafPage(&leaf_buf[0]);
    uint8_t non_leaf_buf[bufsize];
    b.initNonLeafPage(&non_leaf_buf[0]);

    R2PageAccess pa_leaf;
    b.initPageAccess(&pa_leaf, &leaf_buf[0]);
    R2PageAccess pa_non_leaf;
    b.initPageAccess(&pa_non_leaf, &non_leaf_buf[0]);

    union uv {
	uint64_t val64;
	uint32_t val32;
	uint8_t  val8;
    };

    uint8_t a_key;
    ErrorInfo err;
    boost::shared_mutex l;
    bool ok;
    uv val;

    a_key = 0;
    ok = true;
    while (ok) {
	err.clear();
	val.val32 = 1;
	ok = b.blockInsert(&l, &pa_non_leaf, &a_key, &val.val8, &err);
	if (ok)
	    a_key++;
    }
    ASSERT_TRUE(err.haveError == true);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_NODE_FULL);
    ASSERT_TRUE(err.message.find("full") != std::string::npos);

    err.clear();
    a_key = 0;
    ok = b.blockDelete(&l, &pa_non_leaf, &a_key, &err);
    ASSERT_TRUE(ok == true);

    err.clear();
    a_key = 1;
    val.val32 = 1;
    ok = b.blockInsert(&l, &pa_non_leaf, &a_key, &val.val8, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.haveError == true);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_DUPLICATE_INSERT);
    ASSERT_TRUE(err.message.find("duplicate") != std::string::npos);
    
    a_key = 0;
    ok = true;
    while (ok) {
	err.clear();
	val.val64 = 1;
	ok = b.blockInsert(&l, &pa_leaf, &a_key, &val.val8, &err);
	if (ok)
	    a_key++;
    }
    ASSERT_TRUE(err.haveError == true);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_NODE_FULL);
    ASSERT_TRUE(err.message.find("full") != std::string::npos);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree14 : public TestCase {
    TC_R2BTree14() : TestCase("TC_R2BTree14") {;};
    void run();
};

void
TC_R2BTree14::run()
{
    R2ShortKey k;
    // want 3 leaf keys: hdr=8 + val=8*3 + key=1*3 = 33
    const size_t bufsize = 80;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] >= 3);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t nl_b1[bufsize];
    b.initNonLeafPage(&nl_b1[0]);
    uint8_t nl_b2[bufsize];
    b.initNonLeafPage(&nl_b2[0]);

    uint8_t l_b3[bufsize];
    b.initLeafPage(&l_b3[0]);
    uint8_t l_b4[bufsize];
    b.initLeafPage(&l_b4[0]);

    R2PageAccess nl_pa1;
    b.initPageAccess(&nl_pa1, &nl_b1[0]);
    R2PageAccess nl_pa2;
    b.initPageAccess(&nl_pa2, &nl_b2[0]);
    R2PageAccess l_pa3;
    b.initPageAccess(&l_pa3, &l_b3[0]);
    R2PageAccess l_pa4;
    b.initPageAccess(&l_pa4, &l_b4[0]);

    union uv64 {
	uint64_t val64;
	uint8_t  val8;
    };

    uint8_t key;
    ErrorInfo err;
    bool ok;
    boost::shared_mutex l;

    err.clear();
    ok = b.splitNode(NULL, &nl_pa2, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitNode(&nl_pa1, &nl_pa2, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitNode(&l_pa3, &nl_pa2, &key, &err);
    ASSERT_TRUE(ok == false);

    uv64 val;

    key = 0;
    val.val64 = 0;
    while (true) {
	err.clear();
	ok = b.blockInsert(&l, &l_pa3, &key, &val.val8, &err);
	if (!ok)
	    break;
	key++;
	val.val64++;
    }

    err.clear();
    ok = b.splitNode(&l_pa3, NULL, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitNode(&l_pa3, &l_pa3, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitNode(&l_pa3, &l_pa4, NULL, &err);
    ASSERT_TRUE(ok == false);
    
    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree15 : public TestCase {
    TC_R2BTree15() : TestCase("TC_R2BTree15") {;};
    void run();
};

void
TC_R2BTree15::run()
{
    R2ShortKey k;
    const size_t bufsize = 80;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] >= 3);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf1[bufsize];
    b.initLeafPage(&buf1[0]);
    uint8_t buf2[bufsize];
    b.initLeafPage(&buf2[0]);

    R2PageAccess p1;
    b.initPageAccess(&p1, &buf1[0]);
    R2PageAccess p2;
    b.initPageAccess(&p2, &buf2[0]);

    union uv64 {
	uint64_t val64;
	uint8_t  val8;
    };

    uv64 val;
    uint8_t key, mid;
    ErrorInfo err;
    bool ok;
    boost::shared_mutex l;

    key = 0;
    val.val64 = 0;
    while (true) {
	err.clear();
	ok = b.blockInsert(&l, &p1, &key, &val.val8, &err);
	if (!ok)
	    break;
	key++;
	val.val64++;
    }
    ASSERT_TRUE(key == ih.maxNumKeys[PageTypeLeaf]);

    ok = b.splitNode(&p1, &p2, &mid, &err);
    ASSERT_TRUE(ok == true);
    
    for (uint8_t k2 = 0; k2 < mid; k2++) {
	ok = b.blockFind(&l, &p1, &k2, &val.val8, &err);
	ASSERT_TRUE(ok == true);
    }
    for (uint8_t k2 = mid; k2 < ih.maxNumKeys[PageTypeLeaf]; k2++) {
	err.clear();
	ok = b.blockFind(&l, &p1, &k2, &val.val8, &err);
	ASSERT_TRUE(ok == false);
	ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_KEY_NOT_FOUND);
    }

    for (uint8_t k2 = 0; k2 < mid; k2++) {
	err.clear();
	ok = b.blockFind(&l, &p2, &k2, &val.val8, &err);
	ASSERT_TRUE(ok == false);
	ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_KEY_NOT_FOUND);
    }
    for (uint8_t k2 = mid; k2 < ih.maxNumKeys[PageTypeLeaf]; k2++) {
	ok = b.blockFind(&l, &p2, &k2, &val.val8, &err);
	ASSERT_TRUE(ok == true);
    }

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree16 : public TestCase {
    TC_R2BTree16() : TestCase("TC_R2BTree16") {;};
    void run();
};

void
TC_R2BTree16::run()
{
    R2ShortKey k;
    // want 3 leaf keys: hdr=8 + val=8*3 + key=1*3 = 33
    const size_t bufsize = 80;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] >= 3);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t nl_b1[bufsize];
    b.initNonLeafPage(&nl_b1[0]);
    uint8_t nl_b2[bufsize];
    b.initNonLeafPage(&nl_b2[0]);

    uint8_t l_b3[bufsize];
    b.initLeafPage(&l_b3[0]);
    uint8_t l_b4[bufsize];
    b.initLeafPage(&l_b4[0]);

    R2PageAccess nl_pa1;
    b.initPageAccess(&nl_pa1, &nl_b1[0]);
    R2PageAccess nl_pa2;
    b.initPageAccess(&nl_pa2, &nl_b2[0]);
    R2PageAccess l_pa3;
    b.initPageAccess(&l_pa3, &l_b3[0]);
    R2PageAccess l_pa4;
    b.initPageAccess(&l_pa4, &l_b4[0]);

    union uv {
	uint64_t val64;
	uint32_t val32;
	uint8_t  val8;
    };

    uint8_t key;
    ErrorInfo err;
    bool ok;
    boost::shared_mutex l;

    err.clear();
    ok = b.splitNode(NULL, &l_pa3, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitNode(&l_pa3, &l_pa3, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitNode(&nl_pa1, &l_pa3, &key, &err);
    ASSERT_TRUE(ok == false);

    uv child;

    key = 0;
    child.val32 = 0;
    while (true) {
	err.clear();
	ok = b.blockInsert(&l, &nl_pa1, &key, &child.val8, &err);
	if (!ok)
	    break;
	key++;
	child.val32++;
    }

    err.clear();
    ok = b.splitNode(&nl_pa1, NULL, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitNode(&nl_pa1, &l_pa3, &key, &err);
    ASSERT_TRUE(ok == false);

    err.clear();
    ok = b.splitNode(&nl_pa1, &nl_pa2, NULL, &err);
    ASSERT_TRUE(ok == false);
    
    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree17 : public TestCase {
    TC_R2BTree17() : TestCase("TC_R2BTree17") {;};
    void run();
};

void
TC_R2BTree17::run()
{
    R2ShortKey k;
    const size_t bufsize = 80;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeNonLeaf] >= 2);
    ASSERT_TRUE(ih.minNumKeys[PageTypeNonLeaf] > 0);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] >= 3);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf1[bufsize];
    b.initNonLeafPage(&buf1[0]);
    uint8_t buf2[bufsize];
    b.initNonLeafPage(&buf2[0]);

    R2PageAccess p1;
    b.initPageAccess(&p1, &buf1[0]);
    R2PageAccess p2;
    b.initPageAccess(&p2, &buf2[0]);

    union uv64 {
	uint64_t val64;
	uint8_t  val8;
    };

    uv64 val;
    uint8_t key, mid;
    ErrorInfo err;
    bool ok;
    boost::shared_mutex l;

    key = 0;
    val.val64 = 0;
    while (true) {
	err.clear();
	ok = b.blockInsert(&l, &p1, &key, &val.val8, &err);
	if (!ok)
	    break;
	key++;
	val.val64++;
    }
    ASSERT_TRUE(key == ih.maxNumKeys[PageTypeNonLeaf]);

    ok = b.splitNode(&p1, &p2, &mid, &err);
    ASSERT_TRUE(ok == true);
    
    for (uint8_t k2 = 0; k2 < mid; k2++) {
	ok = b.blockFind(&l, &p1, &k2, &val.val8, &err);
	ASSERT_TRUE(ok == true);
    }
    for (uint8_t k2 = mid; k2 < ih.maxNumKeys[PageTypeNonLeaf]; k2++) {
	err.clear();
	ok = b.blockFind(&l, &p1, &k2, &val.val8, &err);
	ASSERT_TRUE(ok == false);
	ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_KEY_NOT_FOUND);
    }

    for (uint8_t k2 = 0; k2 < mid; k2++) {
	err.clear();
	ok = b.blockFind(&l, &p2, &k2, &val.val8, &err);
	ASSERT_TRUE(ok == false);
	ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_KEY_NOT_FOUND);
    }
    for (uint8_t k2 = mid; k2 < ih.maxNumKeys[PageTypeNonLeaf]; k2++) {
	ok = b.blockFind(&l, &p2, &k2, &val.val8, &err);
	ASSERT_TRUE(ok == true);
    }

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree18 : public TestCase {
    TC_R2BTree18() : TestCase("TC_R2BTree18") {;};
    void run();
};

void
TC_R2BTree18::run()
{
    const int n_keys = 20;
    const size_t bufsize = sizeof(PageHeader)
	+ n_keys * (sizeof(uint64_t) + 1); /* keysize */

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2ShortKey k;
    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] == n_keys);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf1[bufsize];
    b.initLeafPage(&buf1[0]);
    uint8_t buf2[bufsize];
    b.initLeafPage(&buf2[0]);

    R2PageAccess p1;
    b.initPageAccess(&p1, &buf1[0]);
    R2PageAccess p2;
    b.initPageAccess(&p2, &buf2[0]);

    union uv64 {
	uint64_t val64;
	uint8_t  val8;
    };

    uv64 val;
    uint8_t key;
    ErrorInfo err;
    bool ok;
    boost::shared_mutex l;

    key = 0;
    val.val64 = 0;
    while (key < 15) {
	err.clear();
	ok = b.blockInsert(&l, &p1, &key, &val.val8, &err);
	ASSERT_TRUE(ok == true);
	ok = b.blockInsert(&l, &p2, &key, &val.val8, &err);
	ASSERT_TRUE(ok == true);
	key++;
	val.val64++;
    }

    bool dstIsFirst = true;

    err.clear();
    ok = b.concatNodes(NULL, &p2, dstIsFirst, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);

    err.clear();
    ok = b.concatNodes(&p1, NULL, dstIsFirst, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);

    err.clear();
    ok = b.concatNodes(&p1, &p2, dstIsFirst, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree19 : public TestCase {
    TC_R2BTree19() : TestCase("TC_R2BTree19") {;};
    void run();
};

void
TC_R2BTree19::run()
{
    const int n_keys = 20;
    const size_t bufsize = sizeof(PageHeader)
	+ n_keys * (sizeof(uint64_t) + 1); /* keysize */
    R2ShortKey k;
    R2IndexHeader ih;

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] == n_keys);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf1[bufsize];
    b.initLeafPage(&buf1[0]);
    uint8_t buf2[bufsize];
    b.initLeafPage(&buf2[0]);

    R2PageAccess p1;
    b.initPageAccess(&p1, &buf1[0]);
    R2PageAccess p2;
    b.initPageAccess(&p2, &buf2[0]);

    union uv64 {
	uint64_t val64;
	uint8_t  val8;
    };

    uv64 val;
    uint8_t key, key2;
    ErrorInfo err;
    bool ok;
    boost::shared_mutex l;

    key = 0;
    val.val64 = 0;
    while (key < 10) {

	err.clear();
	ok = b.blockInsert(&l, &p1, &key, &val.val8, &err);
	ASSERT_TRUE(ok == true);

	key2 = key + 100;
	err.clear();
	ok = b.blockInsert(&l, &p2, &key2, &val.val8, &err);
	ASSERT_TRUE(ok == true);

	key++;
	val.val64++;
    }

    bool dstIsFirst = true;

    err.clear();
    ok = b.concatNodes(&p1, &p2, dstIsFirst, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(p2.header->numKeys == 0);
    ASSERT_TRUE(p1.header->numKeys == 20);

    for (key = 0; key < 10; key++) {
	key2 = key + 100;

	err.clear();
	ok = b.blockFind(&l, &p1, &key, &val.val8, &err);
	ASSERT_TRUE(ok == true);

	err.clear();
	ok = b.blockFind(&l, &p1, &key2, &val.val8, &err);
	ASSERT_TRUE(ok == true);
    }

    p1.header->numKeys = 0;
    p2.header->numKeys = 0;

    key = 0;
    val.val64 = 0;
    while (key < 10) {

	key2 = key + 100;

	err.clear();
	ok = b.blockInsert(&l, &p1, &key2, &val.val8, &err);
	ASSERT_TRUE(ok == true);

	err.clear();
	ok = b.blockInsert(&l, &p2, &key, &val.val8, &err);
	ASSERT_TRUE(ok == true);

	key++;
	val.val64++;
    }
    
    dstIsFirst = false;

    err.clear();
    ok = b.concatNodes(&p1, &p2, dstIsFirst, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(p2.header->numKeys == 0);
    ASSERT_TRUE(p1.header->numKeys == 20);

    for (key = 0; key < 10; key++) {
	key2 = key + 100;

	err.clear();
	ok = b.blockFind(&l, &p1, &key, &val.val8, &err);
	ASSERT_TRUE(ok == true);

	err.clear();
	ok = b.blockFind(&l, &p1, &key2, &val.val8, &err);
	ASSERT_TRUE(ok == true);
    }

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree20 : public TestCase {
    TC_R2BTree20() : TestCase("TC_R2BTree20") {;};
    void run();
};

void
TC_R2BTree20::run()
{
    const int n_keys = 20;
    const size_t bufsize = sizeof(PageHeader)
	+ n_keys * (sizeof(uint64_t) + 1); /* keysize */

    R2BTreeParams params;
    params.pageSize = bufsize;
    params.keySize = 1;
    params.valSize = 8;

    R2ShortKey k;
    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] == n_keys);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf1[bufsize];
    b.initLeafPage(&buf1[0]);
    uint8_t buf2[bufsize];
    b.initNonLeafPage(&buf2[0]);

    R2PageAccess p1;
    b.initPageAccess(&p1, &buf1[0]);
    R2PageAccess p2;
    b.initPageAccess(&p2, &buf2[0]);

    ErrorInfo err;
    bool ok;

    err.clear();
    ok = b.concatNodes(&p1, &p2, true, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree21 : public TestCase {
    TC_R2BTree21() : TestCase("TC_R2BTree21") {;};
    void run();
};

void
TC_R2BTree21::run()
{
    R2BTreeParams params;

    params.keySize = 1;
    params.valSize = 4;

    const int n_keys = 20;
    params.pageSize = sizeof(PageHeader)
	+ n_keys * (params.valSize + params.keySize);

    R2ShortKey k;
    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] == n_keys);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf1[params.pageSize];
    b.initLeafPage(&buf1[0]);
    uint8_t buf2[params.pageSize];
    b.initLeafPage(&buf2[0]);
    uint8_t buf3[params.pageSize];
    b.initNonLeafPage(&buf3[0]);

    R2PageAccess p1;
    b.initPageAccess(&p1, &buf1[0]);
    R2PageAccess p2;
    b.initPageAccess(&p2, &buf2[0]);
    R2PageAccess p3;
    b.initPageAccess(&p3, &buf3[0]);

    ErrorInfo err;
    bool ok;

    err.clear();
    ok = b.redistributeNodes(NULL, &p2, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);

    err.clear();
    ok = b.redistributeNodes(&p1, NULL, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);

    err.clear();
    ok = b.redistributeNodes(&p1, &p3, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);

    err.clear();
    ok = b.redistributeNodes(&p1, &p2, &err);
    ASSERT_TRUE(ok == false);
    ASSERT_TRUE(err.errorNum == ErrorInfo::ERR_BAD_ARG);

    this->setStatus(true);
}

}


/************/

namespace dback {

struct TC_R2BTree22 : public TestCase {
    TC_R2BTree22() : TestCase("TC_R2BTree22") {;};
    void run();
};

void
TC_R2BTree22::run()
{
    R2BTreeParams params;

    params.keySize = 1;
    params.valSize = 4;

    const int n_keys = 20;
    params.pageSize = sizeof(PageHeader)
	+ n_keys * (params.valSize + params.keySize);

    R2ShortKey k;
    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] == n_keys);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf1[params.pageSize];
    b.initLeafPage(&buf1[0]);
    uint8_t buf2[params.pageSize];
    b.initLeafPage(&buf2[0]);

    R2PageAccess p1;
    b.initPageAccess(&p1, &buf1[0]);
    R2PageAccess p2;
    b.initPageAccess(&p2, &buf2[0]);

    union uv {
	uint32_t val32;
	uint8_t  val8;
    } val;
    ErrorInfo err;
    bool ok;
    uint8_t key, key2;
    boost::shared_mutex l;

    for (key = 0; key < ih.maxNumKeys[PageTypeLeaf]; key++) {
	val.val32 = key;
	err.clear();
	ok = b.blockInsert(&l, &p1, &key, &val.val8, &err);
	ASSERT_TRUE(ok == true);
    }

    for (key = 0; key < ih.minNumKeys[PageTypeLeaf] - 1; key++) {
	key2 = 100 + key;
	val.val32 = key2;
	err.clear();
	ok = b.blockInsert(&l, &p2, &key2, &val.val8, &err);
	ASSERT_TRUE(ok == true);
    }

    size_t totKeys = p1.header->numKeys + p2.header->numKeys;

    ok = b.redistributeNodes(&p1, &p2, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(p1.header->numKeys >= b.header->minNumKeys[PageTypeLeaf]);
    ASSERT_TRUE(p2.header->numKeys >= b.header->minNumKeys[PageTypeLeaf]);
    ASSERT_TRUE(totKeys == p1.header->numKeys + p2.header->numKeys);

    for (key = 0; key < p1.header->numKeys; key++) {
	err.clear();
	val.val32 = 0xFFFFffff;
	ok = b.blockFind(&l, &p1, &key, &val.val8, &err);
	ASSERT_TRUE(ok == true);
	ASSERT_TRUE(val.val32 == key);
    }

    for (key = p1.header->numKeys; key < ih.maxNumKeys[PageTypeLeaf]; key++) {
	err.clear();
	val.val32 = 0xFFFFffff;
	ok = b.blockFind(&l, &p2, &key, &val.val8, &err);
	ASSERT_TRUE(ok == true);
	ASSERT_TRUE(val.val32 == key);
    }

    for (key = 0; key < ih.minNumKeys[PageTypeLeaf] - 1; key++) {
	key2 = 100 + key;
	val.val32 = 0xFFFFffff;
	err.clear();
	ok = b.blockFind(&l, &p2, &key2, &val.val8, &err);
	ASSERT_TRUE(ok == true);
	ASSERT_TRUE(val.val32 == key2);
    }

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree23 : public TestCase {
    TC_R2BTree23() : TestCase("TC_R2BTree23") {;};
    void run();
};

void
TC_R2BTree23::run()
{
    R2BTreeParams params;

    params.keySize = 1;
    params.valSize = 4;

    const int n_keys = 20;
    params.pageSize = sizeof(PageHeader)
	+ n_keys * (params.valSize + params.keySize);

    R2ShortKey k;
    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] == n_keys);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf1[params.pageSize];
    b.initLeafPage(&buf1[0]);
    uint8_t buf2[params.pageSize];
    b.initLeafPage(&buf2[0]);

    R2PageAccess p1;
    b.initPageAccess(&p1, &buf1[0]);
    R2PageAccess p2;
    b.initPageAccess(&p2, &buf2[0]);

    union uv {
	uint32_t val32;
	uint8_t  val8;
    } val;
    ErrorInfo err;
    bool ok;
    uint8_t key, key2;
    boost::shared_mutex l;

    for (key = 0; key < ih.minNumKeys[PageTypeLeaf] - 1; key++) {
	val.val32 = key;
	err.clear();
	ok = b.blockInsert(&l, &p1, &key, &val.val8, &err);
	ASSERT_TRUE(ok == true);
    }

    for (key = 0; key < ih.minNumKeys[PageTypeLeaf] + 1; key++) {
	key2 = 100 + key;
	val.val32 = key2;
	err.clear();
	ok = b.blockInsert(&l, &p2, &key2, &val.val8, &err);
	ASSERT_TRUE(ok == true);
    }

    size_t totKeys = p1.header->numKeys + p2.header->numKeys;

    ok = b.redistributeNodes(&p1, &p2, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(p1.header->numKeys >= b.header->minNumKeys[PageTypeLeaf]);
    ASSERT_TRUE(p2.header->numKeys >= b.header->minNumKeys[PageTypeLeaf]);
    ASSERT_TRUE(totKeys == p1.header->numKeys + p2.header->numKeys);

    for (key = 0; key < ih.minNumKeys[PageTypeLeaf] - 1; key++) {
	err.clear();
	val.val32 = 0xFFFFffff;
	ok = b.blockFind(&l, &p1, &key, &val.val8, &err);
	ASSERT_TRUE(ok == true);
	ASSERT_TRUE(val.val32 == key);
    }
	     
    key2 = 100;
    err.clear();
    val.val32 = 0xFFFFffff;
    ok = b.blockFind(&l, &p1, &key2, &val.val8, &err);
    ASSERT_TRUE(ok == true);
    ASSERT_TRUE(val.val32 == key2);


    for (key = 1; key < ih.minNumKeys[PageTypeLeaf] + 1; key++) {
	key2 = 100 + key;
	val.val32 = 0xFFFFffff;
	err.clear();
	ok = b.blockFind(&l, &p2, &key2, &val.val8, &err);
	ASSERT_TRUE(ok == true);
	ASSERT_TRUE(val.val32 == key2);
    }

    this->setStatus(true);
}

}

/************/

namespace dback {

struct TC_R2BTree24 : public TestCase {
    TC_R2BTree24() : TestCase("TC_R2BTree24") {;};
    void run();
};

void
TC_R2BTree24::run()
{
    R2BTreeParams params;

    params.keySize = 1;
    params.valSize = 4;

    const int n_keys = 20;
    params.pageSize = sizeof(PageHeader)
	+ n_keys * (params.valSize + params.keySize);

    R2ShortKey k;
    R2IndexHeader ih;
    R2BTree::initIndexHeader(&ih, &params);
    ASSERT_TRUE(ih.maxNumKeys[PageTypeLeaf] == n_keys);

    R2BTree b;
    b.header = &ih;
    b.root = NULL;
    b.ki = &k;

    uint8_t buf1[params.pageSize];
    b.initLeafPage(&buf1[0]);
    uint8_t buf2[params.pageSize];
    b.initLeafPage(&buf2[0]);

    R2PageAccess p1;
    b.initPageAccess(&p1, &buf1[0]);
    R2PageAccess p2;
    b.initPageAccess(&p2, &buf2[0]);

    union uv {
	uint32_t val32;
	uint8_t  val8;
    } val;
    ErrorInfo err;
    bool ok;
    uint8_t key;
    boost::shared_mutex l;

    for (key = 0; key < ih.minNumKeys[PageTypeLeaf]; key++) {
	val.val32 = key;
	err.clear();
	ok = b.blockInsert(&l, &p1, &key, &val.val8, &err);
	ASSERT_TRUE(ok == true);
	ok = b.blockInsert(&l, &p2, &key, &val.val8, &err);
	ASSERT_TRUE(ok == true);
    }

    ok = b.redistributeNodes(&p1, &p2, &err);
    ASSERT_TRUE(ok == false);

    this->setStatus(true);
}

}

/****************************************************/
/* top level                                        */
/****************************************************/
static TestSuite *
make_suite_all_tests()
{
    TestSuite *s;

    s = new TestSuite();

    s->addTestCase(new TC_Basic01());

    s->addTestCase(new TC_Serial01());
    s->addTestCase(new TC_Serial02());
    s->addTestCase(new TC_Serial03());
    s->addTestCase(new TC_Serial04());
    s->addTestCase(new TC_Serial05());
    s->addTestCase(new TC_Serial06());
    s->addTestCase(new TC_Serial07());
    s->addTestCase(new TC_Serial08());
    s->addTestCase(new TC_Serial09());
    s->addTestCase(new TC_Serial10());
    s->addTestCase(new TC_Serial11());
    s->addTestCase(new TC_Serial12());
    s->addTestCase(new TC_Serial13());
    s->addTestCase(new TC_Serial14());
    s->addTestCase(new TC_Serial15());
    
    s->addTestCase(new dback::TC_BTree00());
    s->addTestCase(new dback::TC_BTree01());
    s->addTestCase(new dback::TC_BTree02());
    s->addTestCase(new dback::TC_BTree03());
    s->addTestCase(new dback::TC_BTree04());
    s->addTestCase(new dback::TC_BTree05());
    s->addTestCase(new dback::TC_BTree06());
    s->addTestCase(new dback::TC_BTree07());
    s->addTestCase(new dback::TC_BTree08());
    s->addTestCase(new dback::TC_BTree09());
    s->addTestCase(new dback::TC_BTree10());
    s->addTestCase(new dback::TC_BTree11());
    s->addTestCase(new dback::TC_BTree12());
    s->addTestCase(new dback::TC_BTree13());
    s->addTestCase(new dback::TC_BTree14());
    s->addTestCase(new dback::TC_BTree15());
    s->addTestCase(new dback::TC_BTree16());
    s->addTestCase(new dback::TC_BTree17());
    s->addTestCase(new dback::TC_BTree18());
    s->addTestCase(new dback::TC_BTree19());

    s->addTestCase(new dback::TC_R2BTree00());
    s->addTestCase(new dback::TC_R2BTree01());
    s->addTestCase(new dback::TC_R2BTree02());
    s->addTestCase(new dback::TC_R2BTree03());
    s->addTestCase(new dback::TC_R2BTree04());
    s->addTestCase(new dback::TC_R2BTree05());
    s->addTestCase(new dback::TC_R2BTree06());
    s->addTestCase(new dback::TC_R2BTree07());
    s->addTestCase(new dback::TC_R2BTree08());
    s->addTestCase(new dback::TC_R2BTree09());
    s->addTestCase(new dback::TC_R2BTree10());
    s->addTestCase(new dback::TC_R2BTree11());
    s->addTestCase(new dback::TC_R2BTree12());
    s->addTestCase(new dback::TC_R2BTree13());
    s->addTestCase(new dback::TC_R2BTree14());
    s->addTestCase(new dback::TC_R2BTree15());
    s->addTestCase(new dback::TC_R2BTree16());
    s->addTestCase(new dback::TC_R2BTree17());
    s->addTestCase(new dback::TC_R2BTree18());
    s->addTestCase(new dback::TC_R2BTree19());
    s->addTestCase(new dback::TC_R2BTree20());
    s->addTestCase(new dback::TC_R2BTree21());
    s->addTestCase(new dback::TC_R2BTree22());
    s->addTestCase(new dback::TC_R2BTree23());
    s->addTestCase(new dback::TC_R2BTree24());

    return s;
}

static TestSuite *
get_named_tests(TestSuite *all, int argc, const char **argv)
{
    TestSuite *s2 = new TestSuite;

    list<TestCase *>::iterator iter = all->tests.begin();
    while (iter != all->tests.end()) {
	for (int i = 1; i < argc; i++) {
	    TestCase *tc = *iter;
	    if (tc->name.compare(argv[i]) == 0) {
		TestCase *tc2 = tc->makeReference();
		s2->addTestCase(tc2);
	    }
	}
	iter++;
    }

    return s2;
}

int
main(int argc, const char **argv)
{
    TestSuite *all = make_suite_all_tests();
    TestSuite *to_run = NULL;
    TestOption opts;
  
    opts.verbose = true;

    if (argc != 1) {
	to_run = get_named_tests(all, argc, argv);
	delete all;
	all = NULL;
    }
    else {
	to_run = all;
	all = NULL;
    }

    TestResult result;
    to_run->run(&result, &opts);

    result.report();
    delete to_run;

    if (result.n_run == result.n_pass
	&& result.n_fail == 0
	&& result.n_exceptions == 0)
	pthread_exit(NULL);
  
    pthread_exit(NULL);
    return 0; /* */
}

/*
  Local Variables:
  mode: c++
  c-basic-offset: 4
  End:
*/
