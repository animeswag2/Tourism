package com.dicoding.tourismapp.core.data

import com.dicoding.tourismapp.core.data.source.remote.network.ApiResponse
import kotlinx.coroutines.flow.*

abstract class NetworkBoundResource<ResultType, RequestType> {

    private var result: Flow<_root_ide_package_.com.dicoding.tourismapp.core.data.Resource<ResultType>> = flow {
        emit(_root_ide_package_.com.dicoding.tourismapp.core.data.Resource.Loading())
        val dbSource = loadFromDB().first()
        if (shouldFetch(dbSource)) {
            emit(_root_ide_package_.com.dicoding.tourismapp.core.data.Resource.Loading())
            when (val apiResponse = createCall().first()) {
                is ApiResponse.Success -> {
                    saveCallResult(apiResponse.data)
                    emitAll(loadFromDB().map {
                        _root_ide_package_.com.dicoding.tourismapp.core.data.Resource.Success(
                            it
                        )
                    })
                }
                is ApiResponse.Empty -> {
                    emitAll(loadFromDB().map {
                        _root_ide_package_.com.dicoding.tourismapp.core.data.Resource.Success(
                            it
                        )
                    })
                }
                is ApiResponse.Error -> {
                    onFetchFailed()
                    emit(
                        _root_ide_package_.com.dicoding.tourismapp.core.data.Resource.Error<ResultType>(
                            apiResponse.errorMessage
                        )
                    )
                }
            }
        } else {
            emitAll(loadFromDB().map {
                _root_ide_package_.com.dicoding.tourismapp.core.data.Resource.Success(
                    it
                )
            })
        }
    }

    protected open fun onFetchFailed() {}

    protected abstract fun loadFromDB(): Flow<ResultType>

    protected abstract fun shouldFetch(data: ResultType?): Boolean

    protected abstract suspend fun createCall(): Flow<ApiResponse<RequestType>>

    protected abstract suspend fun saveCallResult(data: RequestType)

    fun asFlow(): Flow<_root_ide_package_.com.dicoding.tourismapp.core.data.Resource<ResultType>> = result
}