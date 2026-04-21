// object.c — Content-addressable object store

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTATION ─────────────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {

    printf("DEBUG: object_write start\n");

    // Ensure base directories exist
    if (mkdir(PES_DIR, 0755) < 0 && access(PES_DIR, F_OK) != 0) {
        perror("mkdir PES_DIR failed");
        return -1;
    }

    if (mkdir(OBJECTS_DIR, 0755) < 0 && access(OBJECTS_DIR, F_OK) != 0) {
        perror("mkdir OBJECTS_DIR failed");
        return -1;
    }

    // Type string
    const char *type_str;
    if      (type == OBJ_BLOB)   type_str = "blob";
    else if (type == OBJ_TREE)   type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    // Header
    char header[64];
    int hlen = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    if (hlen < 0) return -1;
    int header_len = hlen + 1;

    // Combine header + data
    size_t full_len = header_len + len;
    uint8_t *full = malloc(full_len);
    if (!full) {
        printf("malloc failed\n");
        return -1;
    }

    memcpy(full, header, header_len);
    memcpy(full + header_len, data, len);

    // Compute hash
    compute_hash(full, full_len, id_out);

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);
    printf("DEBUG: hash = %s\n", hex);

    // Dedup
    if (object_exists(id_out)) {
        printf("DEBUG: already exists\n");
        free(full);
        return 0;
    }

    // Create shard directory
    char shard_dir[256];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);

    printf("DEBUG: creating dir %s\n", shard_dir);

    if (mkdir(shard_dir, 0755) < 0 && access(shard_dir, F_OK) != 0) {
        perror("mkdir shard_dir failed");
        free(full);
        return -1;
    }

    // Final path
    char final_path[512];
    snprintf(final_path, sizeof(final_path), "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);

    printf("DEBUG: writing file %s\n", final_path);

    FILE *f = fopen(final_path, "wb");
    if (!f) {
        perror("fopen failed");
        free(full);
        return -1;
    }

    if (fwrite(full, 1, full_len, f) != full_len) {
        perror("fwrite failed");
        fclose(f);
        free(full);
        return -1;
    }

    fclose(f);
    free(full);

    printf("DEBUG: write success\n");

    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {

    char path[512];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    if (size < 0) {
        fclose(f);
        return -1;
    }
    rewind(f);

    uint8_t *buf = malloc(size);
    if (!buf) {
        fclose(f);
        return -1;
    }

    if ((long)fread(buf, 1, size, f) != size) {
        free(buf);
        fclose(f);
        return -1;
    }
    fclose(f);

    ObjectID computed;
    compute_hash(buf, size, &computed);

    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(buf);
        return -1;
    }

    uint8_t *null_pos = memchr(buf, '\0', size);
    if (!null_pos) {
        free(buf);
        return -1;
    }

    if      (strncmp((char *)buf, "blob", 4) == 0) *type_out = OBJ_BLOB;
    else if (strncmp((char *)buf, "tree", 4) == 0) *type_out = OBJ_TREE;
    else if (strncmp((char *)buf, "commit", 6) == 0) *type_out = OBJ_COMMIT;
    else {
        free(buf);
        return -1;
    }

    uint8_t *data_start = null_pos + 1;
    size_t data_len = size - (data_start - buf);

    uint8_t *out = malloc(data_len ? data_len : 1);
    if (!out) {
        free(buf);
        return -1;
    }

    memcpy(out, data_start, data_len);

    *data_out = out;
    *len_out = data_len;

    free(buf);
    return 0;
}
