#include "config.h"

#include <cstddef>
#include <exception>

#include <inttypes.h>

#include "dback.h"

#include <cstdarg>
#include <cstring>

#include <list>
#include <vector>
#include <limits>
#include <sstream>
#include <exception>
#include <iostream>
#include <new>

using namespace std;

#include "dback.h"
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
	    result->tests_e.push_back( tc->name + e.what() );
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
	return 0;
  
    return 1;
}

/*
  Local Variables:
  mode: c++
  c-basic-offset: 4
  End:
*/
