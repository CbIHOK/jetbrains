#ifndef __JB__WINDOWS_POLICY__H__
#define __JB__WINDOWS_POLICY__H__


#include <filesystem>
#include <unordered_map>
#include <limits>


#include <Windows.h>
#undef min
#undef max


namespace jb
{
    struct WindowsPolicy
    {
        /** Declares type of file handle
        */
        using HandleT = HANDLE;


        /** Declares invalid value for file handle
        */
        inline static const HandleT InvalidHandle = INVALID_HANDLE_VALUE;


        /** Opens file for Windows

        @param [in] path - file name to be opened
        @param [in] create - create file if does not exist
        @retval true if the operation succeeds
        @retval true if new file has been created
        @retval handle of opened file
        @throw nothing
        */
        static std::tuple< bool, bool, HandleT > open_file( const std::filesystem::path & path ) noexcept
        {
            using namespace std;

            try
            {
                string p = path.string();

                HandleT handle = ::CreateFileA(
                    p.c_str(),
                    GENERIC_READ | GENERIC_WRITE,
                    FILE_SHARE_READ | FILE_SHARE_WRITE,
                    NULL,
                    OPEN_ALWAYS,
                    FILE_ATTRIBUTE_ARCHIVE | FILE_FLAG_RANDOM_ACCESS | FILE_FLAG_WRITE_THROUGH,
                    NULL );

                auto err = ::GetLastError();
                bool creating = ( err != ERROR_ALREADY_EXISTS );

                return { handle != InvalidHandle, creating, handle };
            }
            catch ( ... )
            {
            }

            return { false, false, InvalidHandle };
        }


        /** Closes file for Windows

        @param [in] handle - file to be closed
        @retval true if the operation succeeds
        @throw nothing
        */
        static std::tuple< bool > close_file( HandleT  handle ) noexcept
        {
            return { TRUE == CloseHandle( handle ) };
        }


        /** Enumerates possible seeking origins
        */
        enum class SeekMethod
        {
            Begin,   ///< Walk from the beginning
            Current, ///< Walk from current possition
            End      ///< Wlak from the end of file
        };


        /** Sets file position for Windows

        @param [in] handle - file to be positioned
        @param [in] offset - number of bytes to be walked through
        @param [in] origin - walking origin
        @retval true if the operation succeeds
        @retval int64_t new position
        @throw nothing
        */

        static std::tuple< bool, int64_t > seek_file( HandleT handle, int64_t offset, SeekMethod origin = SeekMethod::Begin ) noexcept
        {
            try
            {
                static const std::unordered_map< SeekMethod, DWORD > origins{
                    { SeekMethod::Begin, FILE_BEGIN },
                    { SeekMethod::Current, FILE_CURRENT },
                    { SeekMethod::End, FILE_END }
                };

                if ( auto origin_it = origins.find( origin ); origin_it != origins.end() )
                {
                    LARGE_INTEGER li_offset; li_offset.QuadPart = offset;
                    LARGE_INTEGER li_position{};
                    
                    return { TRUE == SetFilePointerEx( 
                            handle, 
                            li_offset, 
                            &li_position, 
                            origin_it->second ), 
                        li_position.QuadPart
                    };
                }
            }
            catch ( ... )
            {
            }

            return { false, 0 };
        }


        /** Implements file writing for Windows

        @param [in] handle - file to be written
        @param [in] buffer - memory buffer to be written
        @param [in] size - amount of bytes to be written
        @retval true if the operation succeeds
        @retval uint64_t - number of written bytes
        @throw nothing
        */
        static std::tuple< bool, uint64_t > write_file( HandleT handle, const void * buffer, size_t size ) noexcept
        {
            if ( size <= std::numeric_limits< DWORD >::max() )
            {
                DWORD written;

                return { 
                    TRUE == WriteFile( handle, 
                        static_cast< LPCVOID >( buffer ), 
                        static_cast< DWORD >( size ), 
                        &written,
                        NULL ) ,
                    written
                };
            }
            else
            {
                return { false, 0 };
            }
        }


        /** Implements file reading for Windows

        @param [in] handle - file to be read
        @param [out] buffer - reading buffer
        @param [in] size - amount of bytes to be read
        @retval bool - true if the operation succeeds
        @retval uint64_t - number of read bytes
        @throw nothing
        */
        static std::tuple< bool, uint64_t > read_file( HandleT handle, void * buffer, size_t size ) noexcept
        {
            if ( size <= std::numeric_limits< DWORD >::max() )
            {
                DWORD read;

                return { 
                    TRUE == ReadFile(
                        handle, 
                        static_cast< __out_data_source( FILE )LPVOID >( buffer ), 
                        static_cast< DWORD >( size ), 
                        &read,
                        NULL ),
                    read
                };
            }
            else
            {
                return { false, 0 };
            }
        }


        /** Implements file resizing for Windows

        @param [in] handle - file to be read
        @param [out] size - desired size
        @retval bool - true if the operation succeeds
        @retval uint64_t - new file size
        @throw nothing
        */
        static std::tuple< bool, uint64_t > resize_file( HandleT handle, uint64_t size ) noexcept
        {
            if ( size > static_cast< uint64_t >( std::numeric_limits< int64_t >::max() ) )
            {
                return { false, 0 };
            }

            if ( auto[ seek_ok, pos ] = seek_file( handle, size ); !seek_ok || pos != size )
            {
                return { false, 0 };
            }

            return { TRUE == SetEndOfFile( handle ), size };
        }
    };
}

#endif