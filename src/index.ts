import debug from "debug";
import cors, { CorsOptions } from "cors";
import express from "express";
import http from "http";
import { Server as SocketIO } from "socket.io";

type UserToFollow = {
  socketId: string;
  username: string;
};
type OnUserFollowedPayload = {
  userToFollow: UserToFollow;
  action: "FOLLOW" | "UNFOLLOW";
};

const serverDebug = debug("server");
const ioDebug = debug("io");
const socketDebug = debug("socket");

require("dotenv").config(
  process.env.NODE_ENV !== "development"
    ? { path: ".env.production" }
    : { path: ".env.development" },
);

const app = express();
const port =
  process.env.PORT || (process.env.NODE_ENV !== "development" ? 80 : 3002); // default port to listen

const parseAllowedOrigins = () => {
  const envOrigins =
    process.env.CORS_ORIGIN?.split(",")
      .map((origin) => origin.trim())
      .filter(Boolean) ?? [];

  if (envOrigins.length > 0) {
    return envOrigins;
  }

  if (process.env.NODE_ENV === "development") {
    return ["http://localhost:3000"];
  }

  return [];
};

const allowedOrigins = parseAllowedOrigins();

const corsOrigin: CorsOptions["origin"] =
  allowedOrigins.length > 0 ? allowedOrigins : true;

const corsOptions: CorsOptions = {
  origin: corsOrigin,
  credentials: true,
};

const ROOM_CODE_LENGTH = Math.min(
  6,
  Math.max(3, Number(process.env.ROOM_CODE_LENGTH ?? 6)),
);
const ROOM_CODE_TTL_MS =
  Number(process.env.ROOM_CODE_TTL_MS) || 1000 * 60 * 60; // default 1h

type RoomCodeEntry = {
  roomId: string;
  roomKey: string;
  expiresAt: number;
};

const roomCodeStore = new Map<string, RoomCodeEntry>();

const cleanupExpiredCodes = () => {
  const now = Date.now();
  roomCodeStore.forEach((entry, code) => {
    if (entry.expiresAt <= now) {
      roomCodeStore.delete(code);
    }
  });
};

setInterval(cleanupExpiredCodes, Math.min(ROOM_CODE_TTL_MS, 1000 * 60 * 5));

const generateShortCode = () => {
  const min = ROOM_CODE_LENGTH === 1 ? 0 : 10 ** (ROOM_CODE_LENGTH - 1);
  const max = 10 ** ROOM_CODE_LENGTH - 1;
  return String(Math.floor(Math.random() * (max - min + 1)) + min).padStart(
    ROOM_CODE_LENGTH,
    "0",
  );
};

app.use(cors(corsOptions));
app.options("*", cors(corsOptions));
app.use(express.static("public"));
app.use(express.json({ limit: "100kb" }));

app.get("/", (req, res) => {
  res.send("Excalidraw collaboration server is up :)");
});

app.post("/api/room-code", (req, res) => {
  const { roomId, roomKey } = req.body ?? {};

  if (typeof roomId !== "string" || typeof roomKey !== "string") {
    return res
      .status(400)
      .json({ error: "roomId and roomKey must be provided as strings" });
  }

  let attempts = 0;
  let code = generateShortCode();

  while (roomCodeStore.has(code) && attempts < 5) {
    code = generateShortCode();
    attempts += 1;
  }

  if (roomCodeStore.has(code)) {
    return res.status(500).json({ error: "Unable to allocate room code" });
  }

  roomCodeStore.set(code, {
    roomId,
    roomKey,
    expiresAt: Date.now() + ROOM_CODE_TTL_MS,
  });

  res.json({ code, expiresInMs: ROOM_CODE_TTL_MS });
});

app.get("/api/room-code/:code", (req, res) => {
  const { code } = req.params;
  if (!code || typeof code !== "string") {
    return res.status(400).json({ error: "Code must be provided" });
  }

  const entry = roomCodeStore.get(code);

  if (!entry) {
    return res.status(404).json({ error: "Code not found" });
  }

  if (entry.expiresAt <= Date.now()) {
    roomCodeStore.delete(code);
    return res.status(404).json({ error: "Code expired" });
  }

  res.json({
    roomId: entry.roomId,
    roomKey: entry.roomKey,
  });
});

const server = http.createServer(app);

server.listen(port, () => {
  serverDebug(`listening on port: ${port}`);
});

try {
  const io = new SocketIO(server, {
    transports: ["websocket", "polling"],
    cors: {
      ...corsOptions,
      allowedHeaders: ["Content-Type", "Authorization"],
    },
    allowEIO3: true,
  });

  io.on("connection", (socket) => {
    ioDebug("connection established!");
    io.to(`${socket.id}`).emit("init-room");
    socket.on("join-room", async (roomID) => {
      socketDebug(`${socket.id} has joined ${roomID}`);
      await socket.join(roomID);
      const sockets = await io.in(roomID).fetchSockets();
      if (sockets.length <= 1) {
        io.to(`${socket.id}`).emit("first-in-room");
      } else {
        socketDebug(`${socket.id} new-user emitted to room ${roomID}`);
        socket.broadcast.to(roomID).emit("new-user", socket.id);
      }

      io.in(roomID).emit(
        "room-user-change",
        sockets.map((socket) => socket.id),
      );
    });

    socket.on(
      "server-broadcast",
      (roomID: string, encryptedData: ArrayBuffer, iv: Uint8Array) => {
        socketDebug(`${socket.id} sends update to ${roomID}`);
        socket.broadcast.to(roomID).emit("client-broadcast", encryptedData, iv);
      },
    );

    socket.on(
      "server-volatile-broadcast",
      (roomID: string, encryptedData: ArrayBuffer, iv: Uint8Array) => {
        socketDebug(`${socket.id} sends volatile update to ${roomID}`);
        socket.volatile.broadcast
          .to(roomID)
          .emit("client-broadcast", encryptedData, iv);
      },
    );

    socket.on("user-follow", async (payload: OnUserFollowedPayload) => {
      const roomID = `follow@${payload.userToFollow.socketId}`;

      switch (payload.action) {
        case "FOLLOW": {
          await socket.join(roomID);

          const sockets = await io.in(roomID).fetchSockets();
          const followedBy = sockets.map((socket) => socket.id);

          io.to(payload.userToFollow.socketId).emit(
            "user-follow-room-change",
            followedBy,
          );

          break;
        }
        case "UNFOLLOW": {
          await socket.leave(roomID);

          const sockets = await io.in(roomID).fetchSockets();
          const followedBy = sockets.map((socket) => socket.id);

          io.to(payload.userToFollow.socketId).emit(
            "user-follow-room-change",
            followedBy,
          );

          break;
        }
      }
    });

    socket.on("disconnecting", async () => {
      socketDebug(`${socket.id} has disconnected`);
      for (const roomID of Array.from(socket.rooms)) {
        const otherClients = (await io.in(roomID).fetchSockets()).filter(
          (_socket) => _socket.id !== socket.id,
        );

        const isFollowRoom = roomID.startsWith("follow@");

        if (!isFollowRoom && otherClients.length > 0) {
          socket.broadcast.to(roomID).emit(
            "room-user-change",
            otherClients.map((socket) => socket.id),
          );
        }

        if (isFollowRoom && otherClients.length === 0) {
          const socketId = roomID.replace("follow@", "");
          io.to(socketId).emit("broadcast-unfollow");
        }
      }
    });

    socket.on("disconnect", () => {
      socket.removeAllListeners();
      socket.disconnect();
    });
  });
} catch (error) {
  console.error(error);
}
