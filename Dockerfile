# This file tells Apify what kind of computer environment to use when running your actor.
# You don't need to change anything here — just make sure this file exists.

FROM apify/actor-node-playwright-chrome:20

# Copy package files first (lets Docker cache dependencies)
COPY package*.json ./

# Install dependencies
RUN npm --quiet set progress=false \
    && npm install --omit=dev --omit=optional \
    && echo "NPM install succeeded"

# Copy the rest of your code
COPY . ./

# This is the command Apify runs to start your actor
CMD npm start
