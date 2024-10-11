#define DUCKDB_EXTENSION_MAIN

#include "quacklm_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <curl/curl.h>
#include <string>
#include <cstring>


typedef std::string string;
 

namespace duckdb {

string make_json_llama3(const string &str){
    string format = "{\"model\": \"llama3\", \"prompt\": \"" +
    str + 
    "\", \"stream\": false}";
    return format;
}

size_t parse_response(void *ptr, size_t size, size_t nmemb, string *s){

    if (nmemb<1){
        return nmemb;
    }
    char* buffer = new char[nmemb+1];
    buffer[nmemb] = 0;

    memcpy(buffer, ptr, size*nmemb);

    s->append(buffer);
    delete [] buffer;
    return nmemb;
}


string_t call_ollama_api(
    const string_t &input,
    const string_t &url
){
    string data = make_json_llama3(input.GetString());
    string result;

    CURL *curl;
    CURLcode res;

    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
    struct curl_slist *headers = NULL;

    headers = curl_slist_append(headers, "Content-Type: text/json");

    curl_easy_setopt(curl, CURLOPT_URL, url.GetString().c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 0L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
    curl_easy_setopt(curl, CURLOPT_TIMEOUT , 120L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, parse_response);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

    auto return_code = curl_easy_perform(curl);

    curl_easy_cleanup(curl);
    curl_global_cleanup();

    if(return_code != CURLE_OK){
        return string_t(curl_easy_strerror(return_code));
    }
    return string_t(result);
} 

inline void QuacklmScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &host_vector = args.data[0];
    auto &input_vector = args.data[1];
    BinaryExecutor::Execute<string_t, string_t, string_t>(
	    host_vector, input_vector, result, args.size(),
	    [&](string_t host, string_t input) {
			return StringVector::AddString(result, call_ollama_api(host, input));;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto quacklm_scalar_function = ScalarFunction("quacklm", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, QuacklmScalarFun);
    ExtensionUtil::RegisterFunction(instance, quacklm_scalar_function);
}

void QuacklmExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string QuacklmExtension::Name() {
	return "quacklm";
}

std::string QuacklmExtension::Version() const {
#ifdef EXT_VERSION_QUACKLM
	return EXT_VERSION_QUACKLM;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void quacklm_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::QuacklmExtension>();
}

DUCKDB_EXTENSION_API const char *quacklm_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
